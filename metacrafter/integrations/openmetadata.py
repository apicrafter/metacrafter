# -*- coding: utf-8 -*-
"""OpenMetadata integration module for exporting Metacrafter scan results."""
import logging
import os
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

try:
    from metadata.ingestion.ometa.ometa_api import OpenMetadata
    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
        OpenMetadataConnection,
    )
    from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
        OpenMetadataJWTClientConfig,
    )
    from metadata.generated.schema.api.data.createTable import CreateTableRequest
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.type.entityReference import EntityReference
    OPENMETADATA_AVAILABLE = True
except ImportError:
    OPENMETADATA_AVAILABLE = False
    logger.warning(
        "OpenMetadata SDK not available. Install with: pip install openmetadata-ingestion"
    )


class OpenMetadataExporter:
    """Export Metacrafter scan results to OpenMetadata metadata catalog.
    
    This class provides functionality to push Metacrafter classification results
    (PII labels, datatypes, confidence scores) to OpenMetadata as tags, glossary terms,
    and custom properties on table columns.
    
    Example:
        ```python
        exporter = OpenMetadataExporter(
            openmetadata_url="http://localhost:8585/api",
            token="your-jwt-token"
        )
        exporter.export_scan_results(
            table_fqn="postgres.default.public.users",
            scan_report=report
        )
        ```
    """
    
    def __init__(
        self,
        openmetadata_url: str,
        token: Optional[str] = None,
        timeout: Optional[float] = None,
        replace: bool = False,
    ):
        """Initialize OpenMetadata exporter.
        
        Args:
            openmetadata_url: OpenMetadata server URL (e.g., "http://localhost:8585/api")
            token: Optional JWT authentication token
            timeout: Optional request timeout in seconds
            replace: If True, replace existing tags/properties instead of merging
            
        Raises:
            ImportError: If OpenMetadata SDK is not installed
        """
        if not OPENMETADATA_AVAILABLE:
            raise ImportError(
                "OpenMetadata SDK is required. Install with: "
                "pip install openmetadata-ingestion"
            )
        
        self.openmetadata_url = openmetadata_url.rstrip('/')
        self.token = token or os.getenv("OPENMETADATA_TOKEN")
        self.timeout = timeout
        self.replace = replace
        
        # Initialize OpenMetadata client
        server_config = OpenMetadataConnection(
            hostPort=self.openmetadata_url,
            authProvider="openmetadata",
            securityConfig=OpenMetadataJWTClientConfig(
                jwtToken=self.token
            ) if self.token else None,
        )
        
        self.metadata = OpenMetadata(server_config)
        
        logger.info(f"Initialized OpenMetadata exporter for {self.openmetadata_url}")
    
    def export_scan_results(
        self,
        table_fqn: str,
        scan_report: Dict[str, Any],
        add_pii_tags: bool = True,
        add_datatype_tags: bool = True,
        link_glossary_terms: bool = True,
        add_properties: bool = True,
        min_confidence: float = 0.0,
    ) -> Dict[str, Any]:
        """Export Metacrafter scan results to OpenMetadata.
        
        Args:
            table_fqn: OpenMetadata table FQN (e.g., "postgres.default.public.users")
            scan_report: Metacrafter scan report dictionary with 'data' field
            add_pii_tags: If True, add PII tags for fields marked as PII
            add_datatype_tags: If True, add tags for detected datatypes
            link_glossary_terms: If True, link glossary terms for datatypes
            add_properties: If True, add custom properties (confidence, URLs)
            min_confidence: Minimum confidence threshold (0-100) for including results
            
        Returns:
            Dictionary with export statistics:
            {
                "fields_processed": int,
                "tags_added": int,
                "glossary_terms_linked": int,
                "properties_added": int,
                "errors": List[str]
            }
        """
        if not OPENMETADATA_AVAILABLE:
            raise ImportError("OpenMetadata SDK is required")
        
        stats = {
            "fields_processed": 0,
            "tags_added": 0,
            "glossary_terms_linked": 0,
            "properties_added": 0,
            "errors": [],
        }
        
        # Extract field data from scan report
        field_data = scan_report.get("data", [])
        if not field_data:
            logger.warning("No field data found in scan report")
            return stats
        
        logger.info(f"Exporting {len(field_data)} fields to OpenMetadata table {table_fqn}")
        
        # Get the table entity first to work with columns
        try:
            table_entity = self.metadata.get_by_name(
                entity=Table,
                fqn=table_fqn,
            )
            if not table_entity:
                error_msg = f"Table not found: {table_fqn}"
                logger.error(error_msg)
                stats["errors"].append(error_msg)
                return stats
        except Exception as e:
            error_msg = f"Error fetching table {table_fqn}: {str(e)}"
            logger.error(error_msg)
            stats["errors"].append(error_msg)
            return stats
        
        for field_info in field_data:
            try:
                field_name = field_info.get("field")
                if not field_name:
                    continue
                
                # Get matches with confidence filtering
                matches = field_info.get("matches", [])
                if not matches:
                    continue
                
                # Filter by minimum confidence
                filtered_matches = [
                    m for m in matches
                    if m.get("confidence", 0.0) >= min_confidence
                ]
                if not filtered_matches:
                    continue
                
                # Get best match (highest confidence)
                best_match = max(filtered_matches, key=lambda x: x.get("confidence", 0.0))
                
                # Collect tags to add
                tags_to_add = []
                if add_pii_tags:
                    pii_tags = self._extract_pii_tags(field_info)
                    tags_to_add.extend(pii_tags)
                
                if add_datatype_tags:
                    datatype_tags = self._extract_datatype_tags(filtered_matches)
                    tags_to_add.extend(datatype_tags)
                
                # Add tags
                if tags_to_add:
                    self._add_tags(table_fqn, field_name, tags_to_add)
                    stats["tags_added"] += len(tags_to_add)
                
                # Link glossary terms
                if link_glossary_terms:
                    glossary_terms = self._extract_glossary_terms(filtered_matches)
                    for term_fqn in glossary_terms:
                        self._link_glossary_term(table_fqn, field_name, term_fqn)
                        stats["glossary_terms_linked"] += 1
                
                # Add custom properties
                if add_properties:
                    properties = self._build_properties(field_info, best_match)
                    if properties:
                        self._add_properties(table_fqn, field_name, properties)
                        stats["properties_added"] += len(properties)
                
                stats["fields_processed"] += 1
                
            except Exception as e:
                error_msg = f"Error processing field {field_info.get('field', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                stats["errors"].append(error_msg)
        
        logger.info(
            f"Export complete: {stats['fields_processed']} fields processed, "
            f"{stats['tags_added']} tags added, "
            f"{stats['glossary_terms_linked']} glossary terms linked"
        )
        
        return stats
    
    def _map_field_to_fqn(self, table_fqn: str, field_name: str) -> str:
        """Convert field name to OpenMetadata column FQN.
        
        Args:
            table_fqn: Table FQN (e.g., "postgres.default.public.users")
            field_name: Field/column name
            
        Returns:
            Column FQN (format: table_fqn.column_name)
        """
        return f"{table_fqn}.{field_name}"
    
    def _extract_pii_tags(self, field_info: Dict[str, Any]) -> List[str]:
        """Extract PII-related tags from field info.
        
        Args:
            field_info: Field information dictionary
            
        Returns:
            List of PII tag names
        """
        tags = []
        
        # Check tags field
        field_tags = field_info.get("tags", [])
        if isinstance(field_tags, str):
            field_tags = [t.strip() for t in field_tags.split(",") if t.strip()]
        
        if "pii" in [t.lower() for t in field_tags]:
            tags.append("PII")
        
        # Check matches for PII indicators
        matches = field_info.get("matches", [])
        for match in matches:
            # Some datatypes are inherently PII
            dataclass = match.get("dataclass", "").lower()
            if dataclass in ["email", "phone", "ssn", "passport", "creditcard"]:
                tags.append("PII")
                break
        
        return list(set(tags))  # Remove duplicates
    
    def _extract_datatype_tags(self, matches: List[Dict[str, Any]]) -> List[str]:
        """Extract datatype tags from matches.
        
        Args:
            matches: List of match dictionaries
            
        Returns:
            List of datatype tag names
        """
        tags = []
        for match in matches:
            dataclass = match.get("dataclass")
            if dataclass:
                # Capitalize first letter for tag name
                tag_name = dataclass[0].upper() + dataclass[1:] if len(dataclass) > 1 else dataclass.upper()
                tags.append(tag_name)
        return list(set(tags))  # Remove duplicates
    
    def _extract_glossary_terms(self, matches: List[Dict[str, Any]]) -> List[str]:
        """Extract glossary term FQNs from matches.
        
        Args:
            matches: List of match dictionaries
            
        Returns:
            List of glossary term FQNs (format: GlossaryTerm.{term_name})
        """
        terms = []
        for match in matches:
            dataclass = match.get("dataclass")
            if dataclass:
                # Generate glossary term FQN
                # OpenMetadata uses FQN format: GlossaryTerm.{term_name}
                term_fqn = f"GlossaryTerm.{dataclass}"
                terms.append(term_fqn)
        return list(set(terms))  # Remove duplicates
    
    def _build_properties(
        self,
        field_info: Dict[str, Any],
        best_match: Dict[str, Any]
    ) -> Dict[str, str]:
        """Build custom properties dictionary for OpenMetadata.
        
        Args:
            field_info: Field information dictionary
            best_match: Best match dictionary (highest confidence)
            
        Returns:
            Dictionary of property key-value pairs
        """
        properties = {}
        
        # Add confidence score
        confidence = best_match.get("confidence")
        if confidence is not None:
            properties["metacrafter_confidence"] = str(confidence)
        
        # Add datatype URL
        datatype_url = field_info.get("datatype_url") or best_match.get("classurl")
        if datatype_url:
            properties["metacrafter_datatype_url"] = datatype_url
        
        # Add datatype name
        dataclass = best_match.get("dataclass")
        if dataclass:
            properties["metacrafter_datatype"] = dataclass
        
        # Add rule ID
        ruleid = best_match.get("ruleid")
        if ruleid:
            properties["metacrafter_rule_id"] = ruleid
        
        # Add field type
        ftype = field_info.get("ftype")
        if ftype:
            properties["metacrafter_field_type"] = ftype
        
        return properties
    
    def _add_tags(self, table_fqn: str, column_name: str, tags: List[str]) -> None:
        """Add tags to an OpenMetadata table column.
        
        Args:
            table_fqn: Table FQN
            column_name: Column name
            tags: List of tag names to add
        """
        if not tags:
            return
        
        try:
            # Use OpenMetadata API to add tags to column
            # The API expects tag FQNs in format: Tag.{tag_name}
            tag_fqns = [f"Tag.{tag_name}" for tag_name in tags]
            
            # Get the table entity
            table_entity = self.metadata.get_by_name(
                entity=Table,
                fqn=table_fqn,
            )
            
            if not table_entity:
                logger.error(f"Table not found: {table_fqn}")
                return
            
            # Find the column
            column = None
            for col in table_entity.columns or []:
                if col.name.__root__ == column_name:
                    column = col
                    break
            
            if not column:
                logger.warning(f"Column {column_name} not found in table {table_fqn}")
                return
            
            # Add tags using patch operation
            # OpenMetadata uses patch operations to update entities
            from metadata.generated.schema.type.tagLabel import TagLabel, TagSource
            
            existing_tags = column.tags or []
            existing_tag_fqns = {tag.tagFQN.__root__ for tag in existing_tags}
            
            new_tags = []
            for tag_fqn in tag_fqns:
                if tag_fqn not in existing_tag_fqns:
                    tag_label = TagLabel(
                        tagFQN=tag_fqn,
                        source=TagSource.Tag,
                        labelType="Manual",
                        state="Suggested"
                    )
                    new_tags.append(tag_label)
            
            if new_tags:
                # Update column with new tags
                column.tags = (existing_tags or []) + new_tags
                
                # Patch the table to update the column
                self.metadata.patch_column(
                    table=table_entity,
                    column=column,
                )
                
                logger.debug(f"Added tags {tags} to column {column_name} in table {table_fqn}")
        except Exception as e:
            logger.error(f"Error adding tags to column {column_name} in table {table_fqn}: {e}")
            # Don't raise - tags are optional
            pass
    
    def _link_glossary_term(self, table_fqn: str, column_name: str, term_fqn: str) -> None:
        """Link a glossary term to an OpenMetadata table column.
        
        Args:
            table_fqn: Table FQN
            column_name: Column name
            term_fqn: Glossary term FQN
        """
        try:
            # Get the table entity
            table_entity = self.metadata.get_by_name(
                entity=Table,
                fqn=table_fqn,
            )
            
            if not table_entity:
                logger.error(f"Table not found: {table_fqn}")
                return
            
            # Find the column
            column = None
            for col in table_entity.columns or []:
                if col.name.__root__ == column_name:
                    column = col
                    break
            
            if not column:
                logger.warning(f"Column {column_name} not found in table {table_fqn}")
                return
            
            # Add glossary term using patch operation
            from metadata.generated.schema.type.entityReference import EntityReference
            from metadata.generated.schema.type.tagLabel import TagLabel, TagSource
            
            # Check if glossary term already exists
            existing_glossary_terms = column.glossaryTerms or []
            existing_term_fqns = {term.name for term in existing_glossary_terms}
            
            # Get glossary term entity to get its ID
            try:
                from metadata.generated.schema.entity.data.glossaryTerm import GlossaryTerm
                glossary_term_entity = self.metadata.get_by_name(
                    entity=GlossaryTerm,
                    fqn=term_fqn,
                )
                
                if not glossary_term_entity:
                    logger.warning(f"Glossary term not found: {term_fqn}")
                    return
                
                if term_fqn not in existing_term_fqns:
                    # Add glossary term reference
                    term_ref = EntityReference(
                        id=glossary_term_entity.id,
                        type="glossaryTerm",
                        name=term_fqn,
                    )
                    
                    column.glossaryTerms = (existing_glossary_terms or []) + [term_ref]
                    
                    # Patch the table to update the column
                    self.metadata.patch_column(
                        table=table_entity,
                        column=column,
                    )
                    
                    logger.debug(f"Linked glossary term {term_fqn} to column {column_name} in table {table_fqn}")
            except Exception as e:
                logger.warning(f"Could not link glossary term {term_fqn}: {e}")
                # Glossary terms may not exist - that's okay
                pass
        except Exception as e:
            logger.error(f"Error linking glossary term {term_fqn} to column {column_name} in table {table_fqn}: {e}")
            # Don't raise - glossary terms are optional
            pass
    
    def _add_properties(self, table_fqn: str, column_name: str, properties: Dict[str, str]) -> None:
        """Add custom properties to an OpenMetadata table column.
        
        Args:
            table_fqn: Table FQN
            column_name: Column name
            properties: Dictionary of property key-value pairs
        """
        if not properties:
            return
        
        try:
            # Get the table entity
            table_entity = self.metadata.get_by_name(
                entity=Table,
                fqn=table_fqn,
            )
            
            if not table_entity:
                logger.error(f"Table not found: {table_fqn}")
                return
            
            # Find the column
            column = None
            for col in table_entity.columns or []:
                if col.name.__root__ == column_name:
                    column = col
                    break
            
            if not column:
                logger.warning(f"Column {column_name} not found in table {table_fqn}")
                return
            
            # Add custom properties
            # OpenMetadata stores custom properties in column.customProperties
            existing_properties = column.customProperties or {}
            
            # Merge with new properties
            updated_properties = {**existing_properties, **properties}
            column.customProperties = updated_properties
            
            # Patch the table to update the column
            self.metadata.patch_column(
                table=table_entity,
                column=column,
            )
            
            logger.debug(f"Added {len(properties)} properties to column {column_name} in table {table_fqn}")
        except Exception as e:
            logger.error(f"Error adding properties to column {column_name} in table {table_fqn}: {e}")
            # Don't raise - properties are optional
            pass


# -*- coding: utf-8 -*-
"""DataHub integration module for exporting Metacrafter scan results."""
import logging
import os
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

try:
    from datahub.emitter.mce_builder import make_schema_field_urn
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        TagAssociationClass,
        GlossaryTermAssociationClass,
        SchemaFieldPropertiesClass,
        GlobalTagsClass,
        GlossaryTermsClass,
        ChangeTypeClass,
    )
    DATAHUB_AVAILABLE = True
except ImportError:
    DATAHUB_AVAILABLE = False
    logger.warning(
        "DataHub SDK not available. Install with: pip install 'acryl-datahub[datahub-rest]'"
    )


class DataHubExporter:
    """Export Metacrafter scan results to DataHub metadata catalog.
    
    This class provides functionality to push Metacrafter classification results
    (PII labels, datatypes, confidence scores) to DataHub as tags, glossary terms,
    and custom properties on dataset schema fields.
    
    Example:
        ```python
        exporter = DataHubExporter(
            datahub_url="http://localhost:8080",
            token="your-token"
        )
        exporter.export_scan_results(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:postgres,users,PROD)",
            scan_report=report
        )
        ```
    """
    
    def __init__(
        self,
        datahub_url: str,
        token: Optional[str] = None,
        timeout: Optional[float] = None,
        replace: bool = False,
    ):
        """Initialize DataHub exporter.
        
        Args:
            datahub_url: DataHub GMS server URL (e.g., "http://localhost:8080")
            token: Optional authentication token
            timeout: Optional request timeout in seconds
            replace: If True, replace existing tags/properties instead of merging
            
        Raises:
            ImportError: If DataHub SDK is not installed
        """
        if not DATAHUB_AVAILABLE:
            raise ImportError(
                "DataHub SDK is required. Install with: "
                "pip install 'acryl-datahub[datahub-rest]'"
            )
        
        self.datahub_url = datahub_url.rstrip('/')
        self.token = token or os.getenv("DATAHUB_TOKEN")
        self.timeout = timeout
        self.replace = replace
        
        # Initialize DataHub REST emitter
        self.emitter = DatahubRestEmitter(
            gms_server=self.datahub_url,
            token=self.token,
            timeout_sec=self.timeout or 30.0,
        )
        
        logger.info(f"Initialized DataHub exporter for {self.datahub_url}")
    
    def export_scan_results(
        self,
        dataset_urn: str,
        scan_report: Dict[str, Any],
        add_pii_tags: bool = True,
        add_datatype_tags: bool = True,
        link_glossary_terms: bool = True,
        add_properties: bool = True,
        min_confidence: float = 0.0,
    ) -> Dict[str, Any]:
        """Export Metacrafter scan results to DataHub.
        
        Args:
            dataset_urn: DataHub dataset URN (e.g., "urn:li:dataset:(platform,name,env)")
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
        if not DATAHUB_AVAILABLE:
            raise ImportError("DataHub SDK is required")
        
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
        
        logger.info(f"Exporting {len(field_data)} fields to DataHub dataset {dataset_urn}")
        
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
                
                # Generate field URN
                field_urn = self._map_field_to_urn(dataset_urn, field_name)
                
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
                    self._add_tags(field_urn, tags_to_add)
                    stats["tags_added"] += len(tags_to_add)
                
                # Link glossary terms
                if link_glossary_terms:
                    glossary_terms = self._extract_glossary_terms(filtered_matches)
                    for term in glossary_terms:
                        self._link_glossary_term(field_urn, term)
                        stats["glossary_terms_linked"] += 1
                
                # Add custom properties
                if add_properties:
                    properties = self._build_properties(field_info, best_match)
                    if properties:
                        self._add_properties(field_urn, properties)
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
    
    def _map_field_to_urn(self, dataset_urn: str, field_name: str) -> str:
        """Convert field name to DataHub schema field URN.
        
        Args:
            dataset_urn: Dataset URN
            field_name: Field/column name
            
        Returns:
            Schema field URN
        """
        return make_schema_field_urn(dataset_urn, field_name)
    
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
        """Extract glossary term URNs from matches.
        
        Args:
            matches: List of match dictionaries
            
        Returns:
            List of glossary term URNs (format: urn:li:glossaryTerm:term_name)
        """
        terms = []
        for match in matches:
            dataclass = match.get("dataclass")
            if dataclass:
                # Generate glossary term URN
                # Assuming terms follow pattern: urn:li:glossaryTerm:datatype_name
                term_urn = f"urn:li:glossaryTerm:{dataclass}"
                terms.append(term_urn)
        return list(set(terms))  # Remove duplicates
    
    def _build_properties(
        self,
        field_info: Dict[str, Any],
        best_match: Dict[str, Any]
    ) -> Dict[str, str]:
        """Build custom properties dictionary for DataHub.
        
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
    
    def _add_tags(self, field_urn: str, tags: List[str]) -> None:
        """Add tags to a DataHub schema field.
        
        Args:
            field_urn: Schema field URN
            tags: List of tag names to add
        """
        if not tags:
            return
        
        try:
            # Create tag associations
            tag_associations = [
                TagAssociationClass(tag=f"urn:li:tag:{tag_name}")
                for tag_name in tags
            ]
            
            # Create GlobalTags aspect
            global_tags = GlobalTagsClass(tags=tag_associations)
            
            # Create MCP for adding tags
            mcp = MetadataChangeProposalWrapper(
                entityType="schemaField",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=field_urn,
                aspectName="globalTags",
                aspect=global_tags,
            )
            
            self.emitter.emit(mcp)
            logger.debug(f"Added tags {tags} to field {field_urn}")
        except Exception as e:
            logger.error(f"Error adding tags to {field_urn}: {e}")
            # Don't raise - tags are optional
            pass
    
    def _link_glossary_term(self, field_urn: str, term_urn: str) -> None:
        """Link a glossary term to a DataHub schema field.
        
        Args:
            field_urn: Schema field URN
            term_urn: Glossary term URN
        """
        try:
            term_association = GlossaryTermAssociationClass(urn=term_urn)
            
            # Create GlossaryTerms aspect
            glossary_terms = GlossaryTermsClass(terms=[term_association])
            
            # Create MCP for adding glossary term
            mcp = MetadataChangeProposalWrapper(
                entityType="schemaField",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=field_urn,
                aspectName="glossaryTerms",
                aspect=glossary_terms,
            )
            
            self.emitter.emit(mcp)
            logger.debug(f"Linked glossary term {term_urn} to field {field_urn}")
        except Exception as e:
            logger.error(f"Error linking glossary term {term_urn} to {field_urn}: {e}")
            # Don't raise - glossary terms are optional
            pass
    
    def _add_properties(self, field_urn: str, properties: Dict[str, str]) -> None:
        """Add custom properties to a DataHub schema field.
        
        Args:
            field_urn: Schema field URN
            properties: Dictionary of property key-value pairs
        """
        if not properties:
            return
        
        try:
            # Create properties aspect
            field_properties = SchemaFieldPropertiesClass(
                customProperties=properties
            )
            
            # Create MCP for adding properties
            mcp = MetadataChangeProposalWrapper(
                entityType="schemaField",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=field_urn,
                aspectName="schemaFieldProperties",
                aspect=field_properties,
            )
            
            self.emitter.emit(mcp)
            logger.debug(f"Added {len(properties)} properties to field {field_urn}")
        except Exception as e:
            logger.error(f"Error adding properties to {field_urn}: {e}")
            # Don't raise - properties are optional
            pass


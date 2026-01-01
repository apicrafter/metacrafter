# -*- coding: utf-8 -*-
"""Apache Atlas integration module for exporting Metacrafter scan results."""
import logging
import os
import base64
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    logger.warning(
        "requests library not available. Install with: pip install requests"
    )


class AtlasExporter:
    """Export Metacrafter scan results to Apache Atlas metadata catalog.
    
    This class provides functionality to push Metacrafter classification results
    (PII labels, datatypes, confidence scores) to Apache Atlas as classifications,
    and custom attributes on column entities.
    
    Example:
        ```python
        exporter = AtlasExporter(
            atlas_url="http://localhost:21000",
            username="admin",
            password="admin"
        )
        exporter.export_scan_results(
            table_qualified_name="postgres.public.users",
            scan_report=report
        )
        ```
    """
    
    def __init__(
        self,
        atlas_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: Optional[float] = None,
        replace: bool = False,
    ):
        """Initialize Apache Atlas exporter.
        
        Args:
            atlas_url: Apache Atlas server URL (e.g., "http://localhost:21000")
            username: Optional username for authentication (or ATLAS_USERNAME env var)
            password: Optional password for authentication (or ATLAS_PASSWORD env var)
            timeout: Optional request timeout in seconds
            replace: If True, replace existing classifications/attributes instead of merging
            
        Raises:
            ImportError: If requests library is not installed
        """
        if not REQUESTS_AVAILABLE:
            raise ImportError(
                "requests library is required. Install with: pip install requests"
            )
        
        self.atlas_url = atlas_url.rstrip('/')
        self.username = username or os.getenv("ATLAS_USERNAME")
        self.password = password or os.getenv("ATLAS_PASSWORD")
        self.timeout = timeout or 30.0
        self.replace = replace
        
        # Prepare authentication headers
        self.auth_headers = {}
        if self.username and self.password:
            credentials = f"{self.username}:{self.password}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            self.auth_headers["Authorization"] = f"Basic {encoded_credentials}"
        
        # Base API URL
        self.api_base = f"{self.atlas_url}/api/atlas/v2"
        
        logger.info(f"Initialized Apache Atlas exporter for {self.atlas_url}")
    
    def export_scan_results(
        self,
        table_qualified_name: str,
        scan_report: Dict[str, Any],
        entity_type: str = "rdbms_column",
        add_pii_classifications: bool = True,
        add_datatype_classifications: bool = True,
        add_attributes: bool = True,
        min_confidence: float = 0.0,
    ) -> Dict[str, Any]:
        """Export Metacrafter scan results to Apache Atlas.
        
        Args:
            table_qualified_name: Atlas table qualified name (e.g., "postgres.public.users")
            scan_report: Metacrafter scan report dictionary with 'data' field
            entity_type: Atlas entity type for columns (default: "rdbms_column")
            add_pii_classifications: If True, add PII classifications for fields marked as PII
            add_datatype_classifications: If True, add classifications for detected datatypes
            add_attributes: If True, add custom attributes (confidence, URLs)
            min_confidence: Minimum confidence threshold (0-100) for including results
            
        Returns:
            Dictionary with export statistics:
            {
                "fields_processed": int,
                "classifications_added": int,
                "attributes_added": int,
                "errors": List[str]
            }
        """
        if not REQUESTS_AVAILABLE:
            raise ImportError("requests library is required")
        
        stats = {
            "fields_processed": 0,
            "classifications_added": 0,
            "attributes_added": 0,
            "errors": [],
        }
        
        # Extract field data from scan report
        field_data = scan_report.get("data", [])
        if not field_data:
            logger.warning("No field data found in scan report")
            return stats
        
        logger.info(f"Exporting {len(field_data)} fields to Apache Atlas table {table_qualified_name}")
        
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
                
                # Resolve column entity GUID
                column_qualified_name = f"{table_qualified_name}.{field_name}"
                column_guid = self._resolve_column_entity(
                    table_qualified_name,
                    field_name,
                    entity_type
                )
                
                if not column_guid:
                    error_msg = f"Column entity not found: {column_qualified_name}"
                    logger.warning(error_msg)
                    stats["errors"].append(error_msg)
                    continue
                
                # Collect classifications to add
                classifications_to_add = []
                if add_pii_classifications:
                    pii_classifications = self._extract_pii_classifications(field_info)
                    classifications_to_add.extend(pii_classifications)
                
                if add_datatype_classifications:
                    datatype_classifications = self._extract_datatype_classifications(filtered_matches)
                    classifications_to_add.extend(datatype_classifications)
                
                # Add classifications
                if classifications_to_add:
                    for classification in classifications_to_add:
                        self._add_classification(column_guid, classification)
                        stats["classifications_added"] += 1
                
                # Add custom attributes
                if add_attributes:
                    attributes = self._build_attributes(field_info, best_match)
                    if attributes:
                        self._add_attributes(column_guid, attributes)
                        stats["attributes_added"] += len(attributes)
                
                stats["fields_processed"] += 1
                
            except Exception as e:
                error_msg = f"Error processing field {field_info.get('field', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                stats["errors"].append(error_msg)
        
        logger.info(
            f"Export complete: {stats['fields_processed']} fields processed, "
            f"{stats['classifications_added']} classifications added, "
            f"{stats['attributes_added']} attributes added"
        )
        
        return stats
    
    def _resolve_column_entity(
        self,
        table_qualified_name: str,
        column_name: str,
        entity_type: str = "rdbms_column"
    ) -> Optional[str]:
        """Resolve column entity GUID by qualified name.
        
        Args:
            table_qualified_name: Table qualified name (e.g., "postgres.public.users")
            column_name: Column name
            entity_type: Atlas entity type (default: "rdbms_column")
            
        Returns:
            Entity GUID or None if not found
        """
        try:
            # Construct column qualified name
            column_qualified_name = f"{table_qualified_name}.{column_name}"
            
            # Try to get entity by unique attribute (qualifiedName)
            url = f"{self.api_base}/entity/uniqueAttribute/type/{entity_type}"
            params = {"attr:qualifiedName": column_qualified_name}
            
            response = requests.get(
                url,
                headers=self.auth_headers,
                params=params,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                entity = response.json()
                return entity.get("entity", {}).get("guid")
            elif response.status_code == 404:
                logger.debug(f"Column entity not found: {column_qualified_name}")
                return None
            else:
                logger.warning(
                    f"Error resolving column entity {column_qualified_name}: "
                    f"HTTP {response.status_code}"
                )
                return None
                
        except Exception as e:
            logger.error(f"Error resolving column entity {column_qualified_name}: {e}")
            return None
    
    def _extract_pii_classifications(self, field_info: Dict[str, Any]) -> List[str]:
        """Extract PII-related classifications from field info.
        
        Args:
            field_info: Field information dictionary
            
        Returns:
            List of PII classification names
        """
        classifications = []
        
        # Check tags field
        field_tags = field_info.get("tags", [])
        if isinstance(field_tags, str):
            field_tags = [t.strip() for t in field_tags.split(",") if t.strip()]
        
        if "pii" in [t.lower() for t in field_tags]:
            classifications.append("PII")
        
        # Check matches for PII indicators
        matches = field_info.get("matches", [])
        for match in matches:
            # Some datatypes are inherently PII
            dataclass = match.get("dataclass", "").lower()
            if dataclass in ["email", "phone", "ssn", "passport", "creditcard"]:
                classifications.append("PII")
                break
        
        return list(set(classifications))  # Remove duplicates
    
    def _extract_datatype_classifications(self, matches: List[Dict[str, Any]]) -> List[str]:
        """Extract datatype classifications from matches.
        
        Args:
            matches: List of match dictionaries
            
        Returns:
            List of datatype classification names
        """
        classifications = []
        for match in matches:
            dataclass = match.get("dataclass")
            if dataclass:
                # Capitalize first letter for classification name
                classification_name = dataclass[0].upper() + dataclass[1:] if len(dataclass) > 1 else dataclass.upper()
                classifications.append(classification_name)
        return list(set(classifications))  # Remove duplicates
    
    def _build_attributes(
        self,
        field_info: Dict[str, Any],
        best_match: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build custom attributes dictionary for Apache Atlas.
        
        Args:
            field_info: Field information dictionary
            best_match: Best match dictionary (highest confidence)
            
        Returns:
            Dictionary of attribute key-value pairs
        """
        attributes = {}
        
        # Add confidence score
        confidence = best_match.get("confidence")
        if confidence is not None:
            attributes["metacrafter_confidence"] = str(confidence)
        
        # Add datatype URL
        datatype_url = field_info.get("datatype_url") or best_match.get("classurl")
        if datatype_url:
            attributes["metacrafter_datatype_url"] = datatype_url
        
        # Add datatype name
        dataclass = best_match.get("dataclass")
        if dataclass:
            attributes["metacrafter_datatype"] = dataclass
        
        # Add rule ID
        ruleid = best_match.get("ruleid")
        if ruleid:
            attributes["metacrafter_rule_id"] = ruleid
        
        # Add field type
        ftype = field_info.get("ftype")
        if ftype:
            attributes["metacrafter_field_type"] = ftype
        
        return attributes
    
    def _add_classification(self, entity_guid: str, classification_name: str) -> None:
        """Add a classification to an Atlas entity.
        
        Args:
            entity_guid: Entity GUID
            classification_name: Classification name to add
        """
        if not classification_name:
            return
        
        try:
            # Check if classification already exists
            existing_classifications = self._get_entity_classifications(entity_guid)
            if classification_name in existing_classifications:
                logger.debug(f"Classification {classification_name} already exists on entity {entity_guid}")
                return
            
            # Add classification
            url = f"{self.api_base}/entity/guid/{entity_guid}/classifications"
            payload = {
                "classification": {
                    "typeName": classification_name,
                    "attributes": {}
                }
            }
            
            response = requests.post(
                url,
                headers={**self.auth_headers, "Content-Type": "application/json"},
                json=payload,
                timeout=self.timeout
            )
            
            if response.status_code in (200, 201):
                logger.debug(f"Added classification {classification_name} to entity {entity_guid}")
            else:
                logger.warning(
                    f"Error adding classification {classification_name} to entity {entity_guid}: "
                    f"HTTP {response.status_code}: {response.text}"
                )
        except Exception as e:
            logger.error(f"Error adding classification {classification_name} to entity {entity_guid}: {e}")
            # Don't raise - classifications are optional
            pass
    
    def _get_entity_classifications(self, entity_guid: str) -> List[str]:
        """Get list of classification names for an entity.
        
        Args:
            entity_guid: Entity GUID
            
        Returns:
            List of classification names
        """
        try:
            url = f"{self.api_base}/entity/guid/{entity_guid}/classifications"
            response = requests.get(
                url,
                headers=self.auth_headers,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                classifications = response.json()
                return [c.get("typeName") for c in classifications.get("list", [])]
            else:
                logger.debug(f"Error getting classifications for entity {entity_guid}: HTTP {response.status_code}")
                return []
        except Exception as e:
            logger.debug(f"Error getting classifications for entity {entity_guid}: {e}")
            return []
    
    def _add_attributes(self, entity_guid: str, attributes: Dict[str, Any]) -> None:
        """Add custom attributes to an Atlas entity.
        
        Args:
            entity_guid: Entity GUID
            attributes: Dictionary of attribute key-value pairs
        """
        if not attributes:
            return
        
        try:
            # Get current entity
            url = f"{self.api_base}/entity/guid/{entity_guid}"
            response = requests.get(
                url,
                headers=self.auth_headers,
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                logger.warning(f"Error getting entity {entity_guid}: HTTP {response.status_code}")
                return
            
            entity = response.json().get("entity", {})
            entity_attributes = entity.get("attributes", {})
            
            # Merge attributes
            if self.replace:
                # Replace existing metacrafter attributes
                filtered_attrs = {
                    k: v for k, v in entity_attributes.items()
                    if not k.startswith("metacrafter_")
                }
                updated_attributes = {**filtered_attrs, **attributes}
            else:
                # Merge with existing attributes
                updated_attributes = {**entity_attributes, **attributes}
            
            # Update entity
            entity["attributes"] = updated_attributes
            
            # Update entity via PUT
            update_url = f"{self.api_base}/entity/guid/{entity_guid}"
            update_response = requests.put(
                update_url,
                headers={**self.auth_headers, "Content-Type": "application/json"},
                json={"entity": entity},
                timeout=self.timeout
            )
            
            if update_response.status_code in (200, 204):
                logger.debug(f"Added {len(attributes)} attributes to entity {entity_guid}")
            else:
                logger.warning(
                    f"Error updating attributes for entity {entity_guid}: "
                    f"HTTP {update_response.status_code}: {update_response.text}"
                )
        except Exception as e:
            logger.error(f"Error adding attributes to entity {entity_guid}: {e}")
            # Don't raise - attributes are optional
            pass


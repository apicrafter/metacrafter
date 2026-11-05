"""OpenMetadata REST export with JSON Patch and confirmed write outcomes.

Using the entity API avoids ingestion SDK dependency conflicts. Taxonomies and
custom properties must already exist on the destination server.
"""
import copy
import os
from typing import Any, Dict, List, Optional
from urllib.parse import quote
import requests
from metacrafter.reports import normalize_report, validate_options
from metacrafter.integrations.outcomes import perform

OPENMETADATA_AVAILABLE = True


class OpenMetadataExporter:
    def __init__(self, openmetadata_url, token=None, timeout=None, replace=False):
        self.openmetadata_url = openmetadata_url.rstrip("/")
        self.token = token or os.environ.get("OPENMETADATA_TOKEN")
        self.timeout = timeout or 30.0
        self.replace = replace
        self.metadata = requests.Session()
        self.metadata.headers.update({"Authorization": "Bearer " + self.token} if self.token else {})
        self._tables = {}

    def _table(self, table_fqn):
        if table_fqn not in self._tables:
            response = self.metadata.get(self.openmetadata_url + "/v1/tables/name/" + quote(table_fqn, safe=""),
                params={"fields": "columns,tags,extension"}, timeout=self.timeout)
            response.raise_for_status()
            table = response.json()
            if not isinstance(table, dict) or not table.get("id") or not isinstance(table.get("columns"), list):
                raise ValueError("Invalid OpenMetadata table response")
            self._tables[table_fqn] = table
        return self._tables[table_fqn]

    def _column(self, table, path):
        def escape(name):
            return name.replace("\\", "\\\\").replace(".", "\\.")
        def visit(columns, prefix="", pointer="/columns"):
            for index, column in enumerate(columns):
                current = prefix + escape(column["name"])
                location = pointer + "/" + str(index)
                if current == path:
                    return column, location
                found = visit(column.get("children", []), current + ".", location + "/children")
                if found:
                    return found
            return None
        found = visit(table["columns"])
        if not found:
            raise ValueError("OpenMetadata column not found")
        return found

    def _patch_column(self, table_fqn, column_name, attribute, value):
        table = self._table(table_fqn)
        column, pointer = self._column(table, column_name)
        if column.get(attribute) == value:
            return False
        # The test protects existing metadata against a concurrent replacement.
        patch = [{"op": "test", "path": "/columns", "value": copy.deepcopy(table["columns"])},
                 {"op": "add", "path": pointer + "/" + attribute, "value": value}]
        response = self.metadata.patch(self.openmetadata_url + "/v1/tables/" + quote(table["id"], safe=""),
            json=patch, headers={"Content-Type": "application/json-patch+json"}, timeout=self.timeout)
        response.raise_for_status()
        column[attribute] = value
        return True

    def _add_tags(self, table_fqn, column_name, tags):
        table = self._table(table_fqn)
        column, _ = self._column(table, column_name)
        current = {tag["tagFQN"]: tag for tag in column.get("tags", [])}
        for tag in tags:
            fqn = "Tag." + tag
            current.setdefault(fqn, {"tagFQN": fqn, "source": "Classification", "labelType": "Manual", "state": "Suggested"})
        return self._patch_column(table_fqn, column_name, "tags", list(current.values()))

    def _link_glossary_term(self, table_fqn, column_name, term_fqn):
        table = self._table(table_fqn)
        column, _ = self._column(table, column_name)
        current = {tag["tagFQN"]: tag for tag in column.get("tags", [])}
        current.setdefault(term_fqn, {"tagFQN": term_fqn, "source": "Glossary", "labelType": "Manual", "state": "Suggested"})
        return self._patch_column(table_fqn, column_name, "tags", list(current.values()))

    def _add_properties(self, table_fqn, column_name, properties):
        table = self._table(table_fqn)
        column, _ = self._column(table, column_name)
        value = dict(column.get("extension") or {}, **properties)
        return self._patch_column(table_fqn, column_name, "extension", value)

    def export_scan_results(self, table_fqn, scan_report, add_pii_tags=True,
                            add_datatype_tags=True, link_glossary_terms=True,
                            add_properties=True, min_confidence=0.0, dry_run=False):
        validate_options(1, min_confidence)
        report = normalize_report(scan_report)
        stats = {"fields_processed": 0, "tags_added": 0, "glossary_terms_linked": 0, "properties_added": 0,
                 "attempted": 0, "succeeded": 0, "skipped": 0, "failed": 0, "planned": [], "errors": []}
        self._tables.clear()
        for field in report["data"]:
            matches = [m for m in field.get("matches", []) if m["confidence"] >= min_confidence]
            if not matches:
                continue
            best = max(matches, key=lambda match: match["confidence"])
            filtered = dict(field, matches=matches, tags=[])
            tags = self._extract_pii_tags(filtered) if add_pii_tags else []
            if add_datatype_tags:
                tags += self._extract_datatype_tags(matches)
            if tags and perform(stats, dry_run, self._add_tags, table_fqn, field["field"], sorted(set(tags))):
                stats["tags_added"] += len(set(tags))
            if link_glossary_terms:
                for term in self._extract_glossary_terms(matches):
                    if perform(stats, dry_run, self._link_glossary_term, table_fqn, field["field"], term):
                        stats["glossary_terms_linked"] += 1
            if add_properties:
                properties = self._build_properties(filtered, best)
                if properties and perform(stats, dry_run, self._add_properties, table_fqn, field["field"], properties):
                    stats["properties_added"] += len(properties)
            stats["fields_processed"] += 1
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
        tags = ["PII"] if any(m.get("is_pii") for m in field_info.get("matches", [])) else []
        
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


# -*- coding: utf-8 -*-
"""Prompt builder for LLM classification."""
from typing import List, Dict, Any, Optional


class PromptBuilder:
    """Build structured prompts for LLM classification."""
    
    @staticmethod
    def build_classification_prompt(
        field_name: str,
        sample_values: Optional[List[str]] = None,
        retrieved_entries: Optional[List[Dict[str, Any]]] = None
    ) -> str:
        """
        Build a classification prompt for the LLM.
        
        Args:
            field_name: Name of the field to classify
            sample_values: Optional list of sample values
            retrieved_entries: List of retrieved registry entries from RAG
            
        Returns:
            Formatted prompt string
        """
        prompt_parts = [
            "You are a semantic data type classifier. Your task is to identify the semantic type of a data field based on its name and sample values.",
            "",
            f"Field Name: {field_name}",
        ]
        
        if sample_values:
            # Limit sample values for prompt
            limited_values = sample_values[:10]  # Use first 10 samples
            values_str = "\n".join(f"  - {v}" for v in limited_values)
            prompt_parts.extend([
                "Sample Values:",
                values_str,
            ])
        
        prompt_parts.append("")
        
        if retrieved_entries:
            prompt_parts.extend([
                "Relevant Registry Entries:",
                "",
            ])
            
            for i, entry in enumerate(retrieved_entries[:10], 1):  # Limit to 10 entries
                metadata = entry.get("metadata", {})
                entry_parts = [
                    f"{i}. ID: {metadata.get('id', 'unknown')}",
                    f"   Name: {metadata.get('name', '')}",
                ]
                
                if metadata.get("doc"):
                    entry_parts.append(f"   Description: {metadata.get('doc')}")
                
                if metadata.get("categories"):
                    entry_parts.append(f"   Categories: {metadata.get('categories')}")
                
                if metadata.get("country"):
                    entry_parts.append(f"   Countries: {metadata.get('country')}")
                
                if metadata.get("langs"):
                    entry_parts.append(f"   Languages: {metadata.get('langs')}")
                
                prompt_parts.extend(entry_parts)
                prompt_parts.append("")
        else:
            prompt_parts.extend([
                "Note: No relevant registry entries were found. Please classify based on the field name and sample values.",
                "",
            ])
        
        prompt_parts.extend([
            "Instructions:",
            "1. Match the field to one of the registry entries above (if provided)",
            "2. Return a valid JSON object with the following structure:",
            '   {"datatype_id": "id_from_registry", "confidence": 0.0-1.0, "reason": "explanation"}',
            "3. If no match is found, return:",
            '   {"datatype_id": null, "confidence": 0.0, "reason": "explanation"}',
            "4. Confidence should be between 0.0 and 1.0, where 1.0 means very confident",
            "",
            "Output (JSON only, no other text):",
        ])
        
        return "\n".join(prompt_parts)
    
    @staticmethod
    def format_registry_entry(entry: Dict[str, Any]) -> str:
        """
        Format a registry entry for display in prompt.
        
        Args:
            entry: Registry entry dictionary
            
        Returns:
            Formatted string
        """
        parts = []
        
        if "id" in entry:
            parts.append(f"ID: {entry['id']}")
        
        if "name" in entry:
            parts.append(f"Name: {entry['name']}")
        
        if "doc" in entry:
            parts.append(f"Description: {entry['doc']}")
        
        if "categories" in entry:
            if isinstance(entry["categories"], list):
                cat_names = [c.get("name", c.get("id", "")) if isinstance(c, dict) else str(c) for c in entry["categories"]]
                parts.append(f"Categories: {', '.join(cat_names)}")
        
        if "country" in entry:
            if isinstance(entry["country"], list):
                country_names = [c.get("name", c.get("id", "")) if isinstance(c, dict) else str(c) for c in entry["country"]]
                parts.append(f"Countries: {', '.join(country_names)}")
        
        if "langs" in entry:
            if isinstance(entry["langs"], list):
                lang_names = [l.get("name", l.get("id", "")) if isinstance(l, dict) else str(l) for l in entry["langs"]]
                parts.append(f"Languages: {', '.join(lang_names)}")
        
        if "examples" in entry and entry["examples"]:
            example_values = []
            for ex in entry["examples"][:3]:  # Limit to 3 examples
                if isinstance(ex, dict):
                    val = ex.get("value", "")
                    if val:
                        example_values.append(val)
                else:
                    example_values.append(str(ex))
            if example_values:
                parts.append(f"Examples: {', '.join(example_values)}")
        
        if "regexp" in entry and entry["regexp"]:
            parts.append(f"Pattern: {entry['regexp']}")
        
        return "\n".join(parts)


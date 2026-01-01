# -*- coding: utf-8 -*-
"""RAG retriever for finding relevant registry entries."""
import logging
from typing import List, Dict, Any, Optional, Union

from .embedder import Embedder
from .vector_store import VectorStore

logger = logging.getLogger(__name__)


class Retriever:
    """Retriever for finding relevant registry entries using RAG."""
    
    def __init__(
        self,
        vector_store: VectorStore,
        embedder: Embedder,
        top_k: int = 10
    ):
        """
        Initialize retriever.
        
        Args:
            vector_store: Vector store instance
            embedder: Embedder instance for query embeddings
            top_k: Number of results to retrieve
        """
        self.vector_store = vector_store
        self.embedder = embedder
        self.top_k = top_k
    
    def retrieve(
        self,
        field_name: str,
        sample_values: Optional[List[str]] = None,
        country: Optional[Union[str, List[str]]] = None,
        langs: Optional[Union[str, List[str]]] = None,
        categories: Optional[Union[str, List[str]]] = None,
        top_k: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve relevant registry entries for a field.
        
        Args:
            field_name: Name of the field to classify
            sample_values: Optional list of sample values from the field
            country: Optional country code(s) to filter by
            langs: Optional language code(s) to filter by
            categories: Optional category ID(s) to filter by
            top_k: Number of results to return (overrides default)
            
        Returns:
            List of retrieved registry entries with metadata
        """
        # Build query text
        query_text = self._build_query(field_name, sample_values)
        
        # Generate query embedding
        try:
            query_embedding = self.embedder.embed_text(query_text)
        except Exception as e:
            logger.error(f"Error generating query embedding: {e}")
            return []
        
        # Build filters
        filters = {}
        if country:
            if isinstance(country, str):
                filters["country"] = country.lower()
            elif isinstance(country, list) and country:
                filters["country"] = country[0].lower()  # ChromaDB where clause supports single value
        
        if langs:
            if isinstance(langs, str):
                filters["langs"] = langs.lower()
            elif isinstance(langs, list) and langs:
                filters["langs"] = langs[0].lower()
        
        if categories:
            if isinstance(categories, str):
                filters["categories"] = categories.lower()
            elif isinstance(categories, list) and categories:
                filters["categories"] = categories[0].lower()
        
        # Search vector store
        k = top_k if top_k is not None else self.top_k
        results = self.vector_store.search(
            query_embedding=query_embedding,
            top_k=k,
            filters=filters if filters else None
        )
        
        logger.debug(f"Retrieved {len(results)} results for field '{field_name}'")
        return results
    
    def _build_query(self, field_name: str, sample_values: Optional[List[str]]) -> str:
        """
        Build query text from field name and sample values.
        
        Args:
            field_name: Field name
            sample_values: Optional list of sample values
            
        Returns:
            Query text string
        """
        parts = [f"Field: {field_name}"]
        
        if sample_values:
            # Limit sample values to avoid overly long queries
            limited_values = sample_values[:5]  # Use first 5 samples
            values_str = ", ".join(str(v) for v in limited_values)
            parts.append(f"Values: {values_str}")
        
        return "\n".join(parts)
    
    def retrieve_by_datatype_id(self, datatype_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific registry entry by datatype ID.
        
        Args:
            datatype_id: Datatype ID to retrieve
            
        Returns:
            Registry entry dictionary or None if not found
        """
        return self.vector_store.get_by_id(datatype_id)


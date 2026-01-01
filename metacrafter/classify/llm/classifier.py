# -*- coding: utf-8 -*-
"""Main LLM classifier interface."""
import os
import logging
from typing import List, Dict, Any, Optional, Union
from pathlib import Path

from .registry_loader import load_registry
from .embedder import Embedder
from .vector_store import VectorStore
from .retriever import Retriever
from .prompt_builder import PromptBuilder
from .llm_client import LLMClient
from .providers import get_provider

logger = logging.getLogger(__name__)

BASE_REGISTRY_URL = "https://registry.apicrafter.io/datatype"


class LLMClassifier:
    """Main classifier interface combining all components."""
    
    def __init__(
        self,
        registry_path: Optional[Union[str, Path]] = None,
        index_path: Optional[Union[str, Path]] = None,
        embedding_model: str = "text-embedding-3-small",
        embedding_api_key: Optional[str] = None,
        llm_provider: str = "openai",
        llm_model: Optional[str] = None,
        llm_api_key: Optional[str] = None,
        llm_base_url: Optional[str] = None,
        top_k: int = 10,
        rebuild_index: bool = False
    ):
        """
        Initialize LLM classifier.
        
        Args:
            registry_path: Path to datatypes_latest.jsonl (or None to use default)
            index_path: Path to vector index directory (or None for in-memory)
            embedding_model: Embedding model to use
            embedding_api_key: OpenAI API key for embeddings (or None to use OPENAI_API_KEY env var)
            llm_provider: LLM provider name (openai, openrouter, ollama, lmstudio, perplexity)
            llm_model: LLM model name (uses provider default if None)
            llm_api_key: LLM provider API key (or None to use provider-specific env var)
            llm_base_url: Base URL for provider (for Ollama, LM Studio custom URLs)
            top_k: Number of registry entries to retrieve for context
            rebuild_index: If True, rebuild index even if it exists
        """
        # Get embedding API key (always uses OpenAI for embeddings)
        self.embedding_api_key = embedding_api_key or os.getenv("OPENAI_API_KEY")
        if not self.embedding_api_key:
            raise ValueError("OpenAI API key required for embeddings. Set OPENAI_API_KEY env var or pass embedding_api_key parameter.")
        
        # Set default registry path if not provided
        if registry_path is None:
            # Try to find registry in common locations
            possible_paths = [
                Path("../metacrafter-registry/data/datatypes_latest.jsonl"),
                Path("../../metacrafter-registry/data/datatypes_latest.jsonl"),
                Path("./datatypes_latest.jsonl"),
            ]
            for path in possible_paths:
                if path.exists():
                    registry_path = path
                    break
            
            if registry_path is None:
                raise ValueError("registry_path required. Could not find default registry file.")
        
        self.registry_path = Path(registry_path)
        self.index_path = Path(index_path) if index_path else None
        self.top_k = top_k
        
        # Initialize embedding components (always uses OpenAI)
        self.embedder = Embedder(
            api_key=self.embedding_api_key,
            model=embedding_model
        )
        
        self.vector_store = VectorStore(
            persist_directory=str(self.index_path) if self.index_path else None
        )
        
        self.retriever = Retriever(
            vector_store=self.vector_store,
            embedder=self.embedder,
            top_k=top_k
        )
        
        # Initialize LLM provider
        provider_kwargs = {}
        if llm_api_key is not None:
            provider_kwargs["api_key"] = llm_api_key
        if llm_base_url is not None:
            provider_kwargs["base_url"] = llm_base_url
        if llm_model is not None:
            provider_kwargs["model"] = llm_model
        
        try:
            provider = get_provider(llm_provider, **provider_kwargs)
            self.llm_client = LLMClient(provider=provider)
        except Exception as e:
            logger.error(f"Failed to initialize LLM provider '{llm_provider}': {e}")
            raise
        
        # Load or build index
        if rebuild_index or self.vector_store.count() == 0:
            logger.info("Building vector index from registry...")
            self._build_index()
        else:
            logger.info(f"Using existing index with {self.vector_store.count()} entries")
    
    def _build_index(self):
        """Build vector index from registry."""
        # Load registry
        datatypes = load_registry(self.registry_path)
        logger.info(f"Loaded {len(datatypes)} datatypes from registry")
        
        if not datatypes:
            raise ValueError(f"No datatypes found in registry: {self.registry_path}")
        
        # Clear existing index
        self.vector_store.clear()
        
        # Generate embeddings
        logger.info("Generating embeddings...")
        embeddings = self.embedder.embed_datatypes(datatypes)
        
        # Prepare metadata
        metadatas = []
        ids = []
        for dt in datatypes:
            metadata = {
                "id": dt.get("id", ""),
                "name": dt.get("name", ""),
                "doc": dt.get("doc", ""),
                "categories": dt.get("categories", []),
                "country": dt.get("country", []),
                "langs": dt.get("langs", []),
            }
            metadatas.append(metadata)
            ids.append(f"datatype_{dt.get('id', 'unknown')}")
        
        # Add to vector store
        logger.info("Adding to vector store...")
        self.vector_store.add_documents(
            embeddings=embeddings,
            metadatas=metadatas,
            ids=ids
        )
        
        logger.info(f"Index built with {len(datatypes)} entries")
    
    def classify_field(
        self,
        field_name: str,
        sample_values: Optional[List[str]] = None,
        country: Optional[Union[str, List[str]]] = None,
        langs: Optional[Union[str, List[str]]] = None,
        categories: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """
        Classify a single field.
        
        Args:
            field_name: Name of the field to classify
            sample_values: Optional list of sample values from the field
            country: Optional country code(s) to filter by
            langs: Optional language code(s) to filter by
            categories: Optional category ID(s) to filter by
            
        Returns:
            Classification result dictionary compatible with Metacrafter format
        """
        # Retrieve relevant registry entries
        retrieved = self.retriever.retrieve(
            field_name=field_name,
            sample_values=sample_values,
            country=country,
            langs=langs,
            categories=categories,
            top_k=self.top_k
        )
        
        # Build prompt
        prompt = PromptBuilder.build_classification_prompt(
            field_name=field_name,
            sample_values=sample_values,
            retrieved_entries=retrieved
        )
        
        # Get LLM classification
        llm_result = self.llm_client.classify(prompt)
        
        # Format result in Metacrafter-compatible format
        datatype_id = llm_result.get("datatype_id")
        confidence = llm_result.get("confidence", 0.0)
        reason = llm_result.get("reason", "")
        
        result = {
            "field": field_name,
            "datatype_id": datatype_id,
            "datatype_url": f"{BASE_REGISTRY_URL}/{datatype_id}" if datatype_id else None,
            "confidence": confidence,
            "reason": reason,
            "matches": [
                {
                    "key": datatype_id or "unknown",
                    "confidence": confidence * 100.0  # Convert to percentage
                }
            ] if datatype_id else []
        }
        
        return result
    
    def classify_batch(
        self,
        fields: List[Dict[str, Any]],
        country: Optional[Union[str, List[str]]] = None,
        langs: Optional[Union[str, List[str]]] = None,
        categories: Optional[Union[str, List[str]]] = None
    ) -> List[Dict[str, Any]]:
        """
        Classify multiple fields in batch.
        
        Args:
            fields: List of field dictionaries, each with 'field_name' and optionally 'sample_values'
            country: Optional country code(s) to filter by
            langs: Optional language code(s) to filter by
            categories: Optional category ID(s) to filter by
            
        Returns:
            List of classification results
        """
        results = []
        for field_info in fields:
            field_name = field_info.get("field_name") or field_info.get("field")
            sample_values = field_info.get("sample_values", [])
            
            result = self.classify_field(
                field_name=field_name,
                sample_values=sample_values,
                country=country,
                langs=langs,
                categories=categories
            )
            results.append(result)
        
        return results
    
    def rebuild_index(self):
        """Rebuild the vector index from registry."""
        logger.info("Rebuilding index...")
        self._build_index()
        logger.info("Index rebuild complete")


# -*- coding: utf-8 -*-
"""Embedding generation for registry datatypes."""
import logging
import math
from typing import List, Dict, Any, Optional
from openai import OpenAI

from .registry_loader import get_datatype_text

logger = logging.getLogger(__name__)


class Embedder:
    """Generate embeddings for registry datatypes using OpenAI."""
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "text-embedding-3-small",
        batch_size: int = 100,
        provider: str = "openai",
        base_url: Optional[str] = None,
        timeout: float = 30.0
    ):
        """
        Initialize embedder.
        
        Args:
            api_key: OpenAI API key (if None, uses OPENAI_API_KEY env var)
            model: Embedding model to use
            batch_size: Number of texts to embed in each batch
        """
        if provider not in ("openai", "ollama", "lmstudio"):
            raise ValueError("Unsupported embedding provider")
        self.provider = provider
        if provider != "openai":
            base_url = base_url or ("http://localhost:11434/v1" if provider == "ollama" else "http://localhost:1234/v1")
            api_key = api_key or "local"
        self.client = OpenAI(api_key=api_key, base_url=base_url, timeout=timeout, max_retries=0)
        self.base_url = base_url
        self.dimension = None
        self.model = model
        self.batch_size = batch_size
    
    def embed_text(self, text: str) -> List[float]:
        """
        Generate embedding for a single text.
        
        Args:
            text: Text to embed
            
        Returns:
            List of embedding values
        """
        try:
            response = self.client.embeddings.create(
                model=self.model,
                input=text
            )
            return self._validate([response.data[0].embedding], 1)[0]
        except Exception as e:
            logger.error("LLM operation failed (%s)", type(e).__name__)
            raise
    
    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for multiple texts in batches.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List of embeddings (each is a list of floats)
        """
        embeddings = []
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i:i + self.batch_size]
            response = self.client.embeddings.create(model=self.model, input=batch)
            embeddings.extend(self._validate([item.embedding for item in response.data], len(batch)))
        return embeddings

    def _validate(self, vectors, expected):
        if len(vectors) != expected:
            raise ValueError("Embedding count mismatch")
        for vector in vectors:
            if not vector or not all(isinstance(v, (float, int)) and math.isfinite(v) for v in vector) or not any(vector):
                raise ValueError("Invalid embedding vector")
            if self.dimension is not None and len(vector) != self.dimension:
                raise ValueError("Embedding dimension changed")
            self.dimension = len(vector)
        return vectors

    def embed_datatypes(self, datatypes: List[Dict[str, Any]]) -> List[List[float]]:
        """
        Generate embeddings for a list of datatypes.
        
        Args:
            datatypes: List of datatype dictionaries
            
        Returns:
            List of embeddings
        """
        texts = [get_datatype_text(dt) for dt in datatypes]
        return self.embed_texts(texts)
    
    def get_embedding_dimension(self) -> int:
        """
        Get the dimension of embeddings for the current model.
        
        Returns:
            Embedding dimension
        """
        # Common dimensions for OpenAI models
        dimensions = {
            "text-embedding-3-small": 1536,
            "text-embedding-3-large": 3072,
            "text-embedding-ada-002": 1536,
        }
        return dimensions.get(self.model, 1536)


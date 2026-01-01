# -*- coding: utf-8 -*-
"""Embedding generation for registry datatypes."""
import logging
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
        batch_size: int = 100
    ):
        """
        Initialize embedder.
        
        Args:
            api_key: OpenAI API key (if None, uses OPENAI_API_KEY env var)
            model: Embedding model to use
            batch_size: Number of texts to embed in each batch
        """
        self.client = OpenAI(api_key=api_key)
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
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Error embedding text: {e}")
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
            try:
                response = self.client.embeddings.create(
                    model=self.model,
                    input=batch
                )
                batch_embeddings = [item.embedding for item in response.data]
                embeddings.extend(batch_embeddings)
                logger.debug(f"Embedded batch {i//self.batch_size + 1}, total: {len(embeddings)}")
            except Exception as e:
                logger.error(f"Error embedding batch starting at index {i}: {e}")
                # Try embedding individually as fallback
                for text in batch:
                    try:
                        emb = self.embed_text(text)
                        embeddings.append(emb)
                    except Exception as e2:
                        logger.error(f"Error embedding individual text: {e2}")
                        # Append zero vector as fallback
                        embeddings.append([0.0] * 1536)  # Default dimension for text-embedding-3-small
        
        return embeddings
    
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


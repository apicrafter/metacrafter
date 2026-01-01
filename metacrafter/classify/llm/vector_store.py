# -*- coding: utf-8 -*-
"""Vector store for registry embeddings using ChromaDB."""
import logging
import os
from typing import List, Dict, Any, Optional
from pathlib import Path
import chromadb
from chromadb.config import Settings

logger = logging.getLogger(__name__)


class VectorStore:
    """Vector store for storing and searching registry embeddings."""
    
    def __init__(
        self,
        persist_directory: Optional[str] = None,
        collection_name: str = "metacrafter_datatypes"
    ):
        """
        Initialize vector store.
        
        Args:
            persist_directory: Directory to persist the database (None = in-memory)
            collection_name: Name of the ChromaDB collection
        """
        self.persist_directory = persist_directory
        self.collection_name = collection_name
        
        # Initialize ChromaDB client
        if persist_directory:
            persist_path = Path(persist_directory)
            persist_path.mkdir(parents=True, exist_ok=True)
            self.client = chromadb.PersistentClient(
                path=str(persist_path),
                settings=Settings(anonymized_telemetry=False)
            )
        else:
            self.client = chromadb.Client(
                settings=Settings(anonymized_telemetry=False)
            )
        
        # Get or create collection
        try:
            self.collection = self.client.get_collection(name=collection_name)
            logger.info(f"Loaded existing collection: {collection_name}")
        except Exception:
            self.collection = self.client.create_collection(name=collection_name)
            logger.info(f"Created new collection: {collection_name}")
    
    def add_documents(
        self,
        embeddings: List[List[float]],
        metadatas: List[Dict[str, Any]],
        ids: Optional[List[str]] = None
    ):
        """
        Add documents to the vector store.
        
        Args:
            embeddings: List of embedding vectors
            metadatas: List of metadata dictionaries (one per embedding)
            ids: Optional list of IDs (if None, will be generated)
        """
        if not ids:
            ids = [f"datatype_{i}" for i in range(len(embeddings))]
        
        # Ensure all metadatas have required fields
        processed_metadatas = []
        for meta in metadatas:
            processed_meta = {
                "id": meta.get("id", ""),
                "name": meta.get("name", ""),
                "doc": meta.get("doc", ""),
            }
            # Add categories, country, langs as comma-separated strings for filtering
            if "categories" in meta and meta["categories"]:
                cat_ids = [_get_id(c) for c in meta["categories"]]
                processed_meta["categories"] = ",".join(cat_ids)
            else:
                processed_meta["categories"] = ""
            
            if "country" in meta and meta["country"]:
                country_ids = [_get_id(c) for c in meta["country"]]
                processed_meta["country"] = ",".join(country_ids)
            else:
                processed_meta["country"] = ""
            
            if "langs" in meta and meta["langs"]:
                lang_ids = [_get_id(l) for l in meta["langs"]]
                processed_meta["langs"] = ",".join(lang_ids)
            else:
                processed_meta["langs"] = ""
            
            processed_metadatas.append(processed_meta)
        
        try:
            self.collection.add(
                embeddings=embeddings,
                metadatas=processed_metadatas,
                ids=ids
            )
            logger.info(f"Added {len(embeddings)} documents to collection")
        except Exception as e:
            logger.error(f"Error adding documents: {e}")
            raise
    
    def search(
        self,
        query_embedding: List[float],
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for similar documents.
        
        Args:
            query_embedding: Query embedding vector
            top_k: Number of results to return
            filters: Optional filters (e.g., {"country": "us", "langs": "en"})
            
        Returns:
            List of result dictionaries with 'id', 'metadata', 'distance', 'datatype'
        """
        # ChromaDB doesn't support $contains, so we need to retrieve more results
        # and filter them post-retrieval for comma-separated string matching
        # Retrieve more results to account for filtering
        query_top_k = top_k * 3 if filters else top_k
        
        try:
            results = self.collection.query(
                query_embeddings=[query_embedding],
                n_results=query_top_k,
                where=None  # Don't use where clause since we'll filter manually
            )
            
            # Format results
            formatted_results = []
            if results["ids"] and len(results["ids"][0]) > 0:
                for i in range(len(results["ids"][0])):
                    metadata = results["metadatas"][0][i]
                    
                    # Apply filters if provided
                    if filters:
                        match = True
                        
                        # Check country filter (comma-separated string)
                        if "country" in filters:
                            country_value = filters["country"].lower()
                            metadata_country = metadata.get("country", "").lower()
                            if not metadata_country or country_value not in metadata_country.split(","):
                                match = False
                        
                        # Check langs filter (comma-separated string)
                        if match and "langs" in filters:
                            langs_value = filters["langs"].lower()
                            metadata_langs = metadata.get("langs", "").lower()
                            if not metadata_langs or langs_value not in metadata_langs.split(","):
                                match = False
                        
                        # Check categories filter (comma-separated string)
                        if match and "categories" in filters:
                            categories_value = filters["categories"].lower()
                            metadata_categories = metadata.get("categories", "").lower()
                            if not metadata_categories or categories_value not in metadata_categories.split(","):
                                match = False
                        
                        if not match:
                            continue
                    
                    result = {
                        "id": results["ids"][0][i],
                        "metadata": metadata,
                        "distance": results["distances"][0][i] if "distances" in results else None,
                        "datatype_id": metadata.get("id", ""),
                    }
                    formatted_results.append(result)
                    
                    # Stop once we have enough filtered results
                    if len(formatted_results) >= top_k:
                        break
            
            return formatted_results
        except Exception as e:
            logger.error(f"Error searching: {e}")
            return []
    
    def get_by_id(self, datatype_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a document by datatype ID.
        
        Args:
            datatype_id: Datatype ID to retrieve
            
        Returns:
            Document dictionary or None if not found
        """
        try:
            results = self.collection.get(
                where={"id": datatype_id}
            )
            if results["ids"]:
                return {
                    "id": results["ids"][0],
                    "metadata": results["metadatas"][0] if results["metadatas"] else {},
                }
            return None
        except Exception as e:
            logger.error(f"Error getting by ID {datatype_id}: {e}")
            return None
    
    def count(self) -> int:
        """Get the number of documents in the collection."""
        try:
            return self.collection.count()
        except Exception as e:
            logger.error(f"Error counting documents: {e}")
            return 0
    
    def clear(self):
        """Clear all documents from the collection."""
        try:
            self.client.delete_collection(name=self.collection_name)
            self.collection = self.client.create_collection(name=self.collection_name)
            logger.info(f"Cleared collection: {self.collection_name}")
        except Exception as e:
            logger.error(f"Error clearing collection: {e}")
            raise


def _get_id(item: Any) -> str:
    """Extract ID from an item (dict or string)."""
    if isinstance(item, dict):
        return item.get("id", str(item))
    return str(item)


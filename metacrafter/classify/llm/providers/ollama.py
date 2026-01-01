# -*- coding: utf-8 -*-
"""Ollama provider implementation."""
import logging
import requests
from typing import Optional

from .base import BaseLLMProvider

logger = logging.getLogger(__name__)


class OllamaProvider(BaseLLMProvider):
    """Ollama local LLM provider."""
    
    def __init__(
        self,
        base_url: str = "http://localhost:11434",
        model: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: float = 30.0
    ):
        """
        Initialize Ollama provider.
        
        Args:
            base_url: Ollama server base URL (default: http://localhost:11434)
            model: Model name (default: llama3)
            max_retries: Maximum number of retries
            retry_delay: Delay between retries
            timeout: Request timeout
        """
        super().__init__(model=model, max_retries=max_retries, retry_delay=retry_delay, timeout=timeout)
        self.base_url = base_url.rstrip('/')
        self.api_url = f"{self.base_url}/api/chat"
    
    def get_default_model(self) -> str:
        """Get default Ollama model."""
        return "llama3"
    
    def supports_json_mode(self) -> bool:
        """Ollama supports JSON format via format parameter."""
        return True
    
    def _call_api(
        self,
        prompt: str,
        temperature: float = 0.0,
        max_tokens: int = 500
    ) -> str:
        """Make API call to Ollama."""
        try:
            payload = {
                "model": self.model,
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a semantic data type classifier. Always respond with valid JSON only."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "stream": False,
                "format": "json"  # Request JSON format
            }
            
            response = requests.post(
                self.api_url,
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            result = response.json()
            return result.get("message", {}).get("content", "")
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Ollama connection error: {e}. Is Ollama running at {self.base_url}?")
            raise
        except requests.exceptions.Timeout as e:
            logger.error(f"Ollama timeout error: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Ollama API error: {e}")
            raise


# -*- coding: utf-8 -*-
"""Perplexity provider implementation."""
import os
import logging
import requests
from typing import Optional

from .base import BaseLLMProvider

logger = logging.getLogger(__name__)


class PerplexityProvider(BaseLLMProvider):
    """Perplexity AI provider."""
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: float = 30.0
    ):
        """
        Initialize Perplexity provider.
        
        Args:
            api_key: Perplexity API key (if None, uses PERPLEXITY_API_KEY env var)
            model: Model name (default: llama-3.1-sonar-small-128k-online)
            max_retries: Maximum number of retries
            retry_delay: Delay between retries
            timeout: Request timeout
        """
        self.api_key = api_key or os.getenv("PERPLEXITY_API_KEY")
        if not self.api_key:
            raise ValueError("Perplexity API key required. Set PERPLEXITY_API_KEY env var or pass api_key parameter.")
        
        super().__init__(model=model, max_retries=max_retries, retry_delay=retry_delay, timeout=timeout)
        self.base_url = "https://api.perplexity.ai"
        self.api_url = f"{self.base_url}/chat/completions"
    
    def get_default_model(self) -> str:
        """Get default Perplexity model."""
        return "llama-3.1-sonar-small-128k-online"
    
    def supports_json_mode(self) -> bool:
        """Perplexity may support JSON mode depending on model."""
        return False  # Perplexity doesn't have explicit JSON mode, parse from text
    
    def _call_api(
        self,
        prompt: str,
        temperature: float = 0.0,
        max_tokens: int = 500
    ) -> str:
        """Make API call to Perplexity."""
        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
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
                "temperature": temperature,
                "max_tokens": max_tokens
            }
            
            response = requests.post(
                self.api_url,
                headers=headers,
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            result = response.json()
            return result["choices"][0]["message"]["content"]
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Perplexity connection error: {e}")
            raise
        except requests.exceptions.Timeout as e:
            logger.error(f"Perplexity timeout error: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Perplexity API error: {e}")
            raise


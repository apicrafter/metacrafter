# -*- coding: utf-8 -*-
"""OpenRouter provider implementation."""
import os
import logging
from typing import Optional
from openai import OpenAI
from openai import APIError, RateLimitError, APIConnectionError

from .base import BaseLLMProvider

logger = logging.getLogger(__name__)


class OpenRouterProvider(BaseLLMProvider):
    """OpenRouter API provider."""
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: float = 30.0,
        http_referer: Optional[str] = None,
        x_title: Optional[str] = None
    ):
        """
        Initialize OpenRouter provider.
        
        Args:
            api_key: OpenRouter API key (if None, uses OPENROUTER_API_KEY env var)
            model: Model name (default: openai/gpt-4o-mini)
            max_retries: Maximum number of retries
            retry_delay: Delay between retries
            timeout: Request timeout
            http_referer: HTTP Referer header (default: https://github.com/apicrafter/metacrafter)
            x_title: X-Title header (default: Metacrafter)
        """
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError("OpenRouter API key required. Set OPENROUTER_API_KEY env var or pass api_key parameter.")
        
        super().__init__(model=model, max_retries=max_retries, retry_delay=retry_delay, timeout=timeout)
        
        # OpenRouter requires specific headers
        self.http_referer = http_referer or "https://github.com/apicrafter/metacrafter"
        self.x_title = x_title or "Metacrafter"
        
        # Use OpenAI client with OpenRouter base URL
        self.client = OpenAI(
            api_key=self.api_key,
            base_url="https://openrouter.ai/api/v1",
            timeout=self.timeout,
            default_headers={
                "HTTP-Referer": self.http_referer,
                "X-Title": self.x_title,
            }
        )
    
    def get_default_model(self) -> str:
        """Get default OpenRouter model."""
        return "openai/gpt-4o-mini"
    
    def supports_json_mode(self) -> bool:
        """OpenRouter supports JSON mode (depends on model)."""
        return True
    
    def _call_api(
        self,
        prompt: str,
        temperature: float = 0.0,
        max_tokens: int = 500
    ) -> str:
        """Make API call to OpenRouter."""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a semantic data type classifier. Always respond with valid JSON only."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=temperature,
                max_tokens=max_tokens,
                response_format={"type": "json_object"}
            )
            
            return response.choices[0].message.content
        except RateLimitError as e:
            logger.warning(f"OpenRouter rate limit error: {e}")
            raise
        except APIConnectionError as e:
            logger.warning(f"OpenRouter connection error: {e}")
            raise
        except APIError as e:
            logger.error(f"OpenRouter API error: {e}")
            raise


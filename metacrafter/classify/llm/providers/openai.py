# -*- coding: utf-8 -*-
"""OpenAI provider implementation."""
import os
import logging
from typing import Optional
from openai import OpenAI
from openai import APIError, RateLimitError, APIConnectionError

from .base import BaseLLMProvider

logger = logging.getLogger(__name__)


class OpenAIProvider(BaseLLMProvider):
    """OpenAI API provider."""
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: float = 30.0
    ):
        """
        Initialize OpenAI provider.
        
        Args:
            api_key: OpenAI API key (if None, uses OPENAI_API_KEY env var)
            model: Model name (default: gpt-4o-mini)
            max_retries: Maximum number of retries
            retry_delay: Delay between retries
            timeout: Request timeout
        """
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OpenAI API key required. Set OPENAI_API_KEY env var or pass api_key parameter.")
        
        super().__init__(model=model, max_retries=max_retries, retry_delay=retry_delay, timeout=timeout)
        self.client = OpenAI(api_key=self.api_key, timeout=self.timeout)
    
    def get_default_model(self) -> str:
        """Get default OpenAI model."""
        return "gpt-4o-mini"
    
    def supports_json_mode(self) -> bool:
        """OpenAI supports JSON mode."""
        return True
    
    def _call_api(
        self,
        prompt: str,
        temperature: float = 0.0,
        max_tokens: int = 500
    ) -> str:
        """Make API call to OpenAI."""
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
            logger.warning(f"OpenAI rate limit error: {e}")
            raise
        except APIConnectionError as e:
            logger.warning(f"OpenAI connection error: {e}")
            raise
        except APIError as e:
            logger.error(f"OpenAI API error: {e}")
            raise


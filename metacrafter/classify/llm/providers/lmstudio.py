# -*- coding: utf-8 -*-
"""LM Studio provider implementation."""
import logging
from typing import Optional
from openai import OpenAI
from openai import APIError, RateLimitError, APIConnectionError

from .base import BaseLLMProvider

logger = logging.getLogger(__name__)


class LMStudioProvider(BaseLLMProvider):
    """LM Studio local LLM provider (OpenAI-compatible)."""
    
    def __init__(
        self,
        base_url: str = "http://localhost:1234/v1",
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: float = 30.0
    ):
        """
        Initialize LM Studio provider.
        
        Args:
            base_url: LM Studio server base URL (default: http://localhost:1234/v1)
            api_key: API key (not required, but may need dummy value like "lm-studio")
            model: Model name (default: local-model)
            max_retries: Maximum number of retries
            retry_delay: Delay between retries
            timeout: Request timeout
        """
        super().__init__(model=model, max_retries=max_retries, retry_delay=retry_delay, timeout=timeout)
        # LM Studio may require a dummy API key
        self.api_key = api_key or "lm-studio"
        self.base_url = base_url.rstrip('/')
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=self.timeout
        )
    
    def get_default_model(self) -> str:
        """Get default LM Studio model."""
        return "local-model"
    
    def supports_json_mode(self) -> bool:
        """LM Studio supports JSON mode (OpenAI-compatible)."""
        return True
    
    def _call_api(
        self,
        prompt: str,
        temperature: float = 0.0,
        max_tokens: int = 500
    ) -> str:
        """Make API call to LM Studio."""
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
        except APIConnectionError as e:
            logger.error(f"LM Studio connection error: {e}. Is LM Studio server running at {self.base_url}?")
            raise
        except RateLimitError as e:
            logger.warning(f"LM Studio rate limit error: {e}")
            raise
        except APIError as e:
            logger.error(f"LM Studio API error: {e}")
            raise


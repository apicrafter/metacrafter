# -*- coding: utf-8 -*-
"""Provider-agnostic LLM client wrapper."""
import logging
from typing import Dict, Any, List, Optional

from .providers.base import BaseLLMProvider

logger = logging.getLogger(__name__)


class LLMClient:
    """Provider-agnostic LLM client wrapper."""
    
    def __init__(
        self,
        provider: BaseLLMProvider,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        """
        Initialize LLM client.
        
        Args:
            provider: LLM provider instance
            max_retries: Maximum number of retries for failed requests
            retry_delay: Delay between retries in seconds
        """
        self.provider = provider
        self.max_retries = max_retries
        self.retry_delay = retry_delay
    
    def classify(
        self,
        prompt: str,
        temperature: float = 0.0,
        max_tokens: int = 500
    ) -> Dict[str, Any]:
        """
        Classify a field using the LLM.
        
        Args:
            prompt: Classification prompt
            temperature: Sampling temperature (0.0 for deterministic)
            max_tokens: Maximum tokens in response
            
        Returns:
            Dictionary with 'datatype_id', 'confidence', 'reason'
        """
        return self.provider.classify(prompt, temperature, max_tokens)
    
    def classify_batch(
        self,
        prompts: List[str],
        temperature: float = 0.0,
        max_tokens: int = 500
    ) -> List[Dict[str, Any]]:
        """
        Classify multiple fields in batch.
        
        Args:
            prompts: List of classification prompts
            temperature: Sampling temperature
            max_tokens: Maximum tokens per response
            
        Returns:
            List of classification results
        """
        return self.provider.classify_batch(prompts, temperature, max_tokens)


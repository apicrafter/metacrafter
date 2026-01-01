# -*- coding: utf-8 -*-
"""Base LLM provider abstract class."""
import json
import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class BaseLLMProvider(ABC):
    """Abstract base class for LLM providers."""
    
    def __init__(
        self,
        model: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: float = 30.0
    ):
        """
        Initialize provider.
        
        Args:
            model: Model name (uses default if None)
            max_retries: Maximum number of retries for failed requests
            retry_delay: Delay between retries in seconds
            timeout: Request timeout in seconds
        """
        self.model = model or self.get_default_model()
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
    
    @abstractmethod
    def get_default_model(self) -> str:
        """Get the default model for this provider."""
        pass
    
    @abstractmethod
    def supports_json_mode(self) -> bool:
        """Whether this provider supports JSON mode."""
        pass
    
    @abstractmethod
    def _call_api(
        self,
        prompt: str,
        temperature: float = 0.0,
        max_tokens: int = 500
    ) -> str:
        """
        Make API call to the provider.
        
        Args:
            prompt: Classification prompt
            temperature: Sampling temperature
            max_tokens: Maximum tokens in response
            
        Returns:
            Response text content
        """
        pass
    
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
        for attempt in range(self.max_retries):
            try:
                content = self._call_api(prompt, temperature, max_tokens)
                
                # Parse JSON response
                result = self._parse_response(content)
                
                # Validate structure
                if "datatype_id" not in result:
                    logger.warning("Response missing 'datatype_id' field")
                    result["datatype_id"] = None
                
                if "confidence" not in result:
                    logger.warning("Response missing 'confidence' field")
                    result["confidence"] = 0.0
                else:
                    # Ensure confidence is between 0 and 1
                    result["confidence"] = max(0.0, min(1.0, float(result["confidence"])))
                
                if "reason" not in result:
                    result["reason"] = ""
                
                return result
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON response (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue
                # Return default on final failure
                return {
                    "datatype_id": None,
                    "confidence": 0.0,
                    "reason": f"Failed to parse JSON response: {e}"
                }
            
            except Exception as e:
                logger.error(f"Error in classification (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.info(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    continue
                # Return default on final failure
                return {
                    "datatype_id": None,
                    "confidence": 0.0,
                    "reason": f"Error: {str(e)}"
                }
        
        # Should not reach here, but return default if we do
        return {
            "datatype_id": None,
            "confidence": 0.0,
            "reason": "Max retries exceeded"
        }
    
    def _parse_response(self, content: str) -> Dict[str, Any]:
        """
        Parse response content to extract JSON.
        
        Args:
            content: Raw response content
            
        Returns:
            Parsed JSON dictionary
        """
        # Try to extract JSON from response (in case provider doesn't support JSON mode)
        content = content.strip()
        
        # Try to find JSON object in the response
        if content.startswith("{"):
            # Response starts with JSON
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                pass
        
        # Try to find JSON object within the response
        start_idx = content.find("{")
        end_idx = content.rfind("}")
        if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
            json_str = content[start_idx:end_idx + 1]
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                pass
        
        # If all else fails, try parsing the whole content
        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from response: {content[:200]}")
            raise
    
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
        results = []
        for i, prompt in enumerate(prompts):
            logger.debug(f"Classifying field {i + 1}/{len(prompts)}")
            result = self.classify(prompt, temperature=temperature, max_tokens=max_tokens)
            results.append(result)
            # Small delay to avoid rate limits
            if i < len(prompts) - 1:
                time.sleep(0.1)
        
        return results


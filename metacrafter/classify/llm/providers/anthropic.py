# -*- coding: utf-8 -*-
"""Anthropic (Claude) provider using the official ``anthropic`` SDK.

The answer is constrained with structured outputs (``output_config.format``) so the
response is a JSON object that matches the classification schema. Server-side refusal
fallbacks are enabled by default (``fallbacks="default"``); a refusal from the whole
chain is reported as an error, never parsed as a classification.
"""
import json
import logging
from typing import Any, Dict, Optional

from metacrafter.exceptions import require_extra

from .base import BaseLLMProvider

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "claude-opus-5-5"
FALLBACK_BETA = "server-side-fallback-2026-07-01"
SYSTEM_PROMPT = "You are a semantic data type classifier. Follow the output schema exactly."


class AnthropicProvider(BaseLLMProvider):
    """Claude via the Messages API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        base_url: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: float = 60.0,
        effort: str = "medium",
        fallbacks: bool = True,
    ):
        anthropic = require_extra("anthropic", "llm", "The Anthropic provider")
        super().__init__(model=model, max_retries=max_retries, retry_delay=retry_delay, timeout=timeout)
        kwargs: Dict[str, Any] = {"timeout": timeout, "max_retries": 2}
        if api_key:
            kwargs["api_key"] = api_key
        if base_url:
            kwargs["base_url"] = base_url
        # Without api_key the SDK resolves ANTHROPIC_API_KEY, ANTHROPIC_AUTH_TOKEN or an
        # `ant auth login` profile.
        self.client = anthropic.Anthropic(**kwargs)
        self.effort = effort
        self.fallbacks = fallbacks

    def get_default_model(self) -> str:
        return DEFAULT_MODEL

    def supports_json_mode(self) -> bool:
        return True

    def _call_api(self, prompt: str, temperature: float = 0.0, max_tokens: int = 500, schema=None) -> str:
        # Thinking is always on for current models and counts toward max_tokens, so leave
        # room beyond the short JSON answer. Sampling parameters are not sent.
        params: Dict[str, Any] = {
            "model": self.model,
            "max_tokens": max(max_tokens, 4096),
            "system": SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": prompt}],
        }
        output_config: Dict[str, Any] = {"effort": self.effort}
        if schema is not None:
            output_config["format"] = {"type": "json_schema", "schema": schema}
        params["output_config"] = output_config
        if self.fallbacks:
            response = self.client.beta.messages.create(
                betas=[FALLBACK_BETA], fallbacks="default", **params
            )
        else:
            response = self.client.messages.create(**params)
        if response.stop_reason == "refusal":
            category = getattr(getattr(response, "stop_details", None), "category", None)
            raise RuntimeError(f"Model declined the classification request (category: {category})")
        if response.stop_reason == "max_tokens":
            raise RuntimeError("Model response was cut off at max_tokens")
        text = next((block.text for block in response.content if block.type == "text"), "")
        if not text:
            raise RuntimeError("Model returned no text")
        # Validate early so a malformed answer is retried by BaseLLMProvider.classify.
        json.loads(text)
        return text

# -*- coding: utf-8 -*-
"""One client for every OpenAI-compatible chat completions API.

Presets set defaults for OpenAI, OpenRouter, Perplexity, Ollama and LM Studio;
``base_url``, ``model`` and ``api_key`` override them. Structured output uses
``response_format: json_schema`` and falls back to ``json_object`` (then plain text)
when a server does not support it.
"""
import logging
import os
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import requests

from .base import BaseLLMProvider

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "You are a semantic data type classifier. Answer with a single JSON object "
    "and nothing else."
)

PRESETS: Dict[str, Dict[str, Any]] = {
    "openai": {"base_url": "https://api.openai.com/v1", "env": "OPENAI_API_KEY", "model": "gpt-4o-mini", "key_required": True},
    "openrouter": {"base_url": "https://openrouter.ai/api/v1", "env": "OPENROUTER_API_KEY", "model": "openai/gpt-4o-mini", "key_required": True},
    "perplexity": {"base_url": "https://api.perplexity.ai", "env": "PERPLEXITY_API_KEY", "model": "llama-3.1-sonar-small-128k-online", "key_required": True},
    "ollama": {"base_url": "http://localhost:11434/v1", "env": None, "model": "llama3", "key_required": False},
    "lmstudio": {"base_url": "http://localhost:1234/v1", "env": None, "model": "local-model", "key_required": False},
}

LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}


def is_local_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host in LOCAL_HOSTS or host.endswith(".localhost")


class OpenAICompatibleProvider(BaseLLMProvider):
    """Chat completions over HTTP for any OpenAI-compatible server."""

    preset = "openai"

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        base_url: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: float = 30.0,
        preset: Optional[str] = None,
    ):
        self.preset = preset or self.preset
        if self.preset not in PRESETS:
            raise ValueError(f"Unknown preset: {self.preset}")
        settings = PRESETS[self.preset]
        self.api_key = api_key or (os.getenv(settings["env"]) if settings["env"] else None)
        if settings["key_required"] and not self.api_key:
            raise ValueError(
                f"{self.preset} API key required. Set {settings['env']} env var or pass api_key parameter."
            )
        self.base_url = (base_url or settings["base_url"]).rstrip("/")
        self.is_local = is_local_url(self.base_url)
        self._response_format_mode = "json_schema"
        super().__init__(model=model, max_retries=max_retries, retry_delay=retry_delay, timeout=timeout)

    def get_default_model(self) -> str:
        return PRESETS[self.preset]["model"]

    def supports_json_mode(self) -> bool:
        return True

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _response_format(self, schema: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if self._response_format_mode == "json_schema" and schema is not None:
            return {"type": "json_schema", "json_schema": {"name": "classification", "schema": schema, "strict": True}}
        if self._response_format_mode in ("json_schema", "json_object"):
            return {"type": "json_object"}
        return None

    def _call_api(self, prompt: str, temperature: float = 0.0, max_tokens: int = 500, schema=None) -> str:
        while True:
            payload: Dict[str, Any] = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                "temperature": temperature,
                "max_tokens": max_tokens,
            }
            response_format = self._response_format(schema)
            if response_format is not None:
                payload["response_format"] = response_format
            try:
                response = requests.post(
                    f"{self.base_url}/chat/completions",
                    json=payload,
                    headers=self._headers(),
                    timeout=self.timeout,
                )
            except requests.exceptions.ConnectionError as exc:
                logger.error("Connection to %s failed. Is the server running?", self.base_url)
                raise ConnectionError(f"Could not connect to {self.base_url}") from exc
            if response.status_code == 400 and response_format is not None:
                # The server does not accept this response_format: degrade and retry once per level.
                self._response_format_mode = "json_object" if self._response_format_mode == "json_schema" else "none"
                logger.info("Retrying without %s response format", response_format["type"])
                continue
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"] or ""

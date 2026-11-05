# -*- coding: utf-8 -*-
"""OpenAIProvider: the `openai` preset of the OpenAI-compatible client."""
from .openai_compatible import OpenAICompatibleProvider


class OpenAIProvider(OpenAICompatibleProvider):
    """OpenAI-compatible client with `openai` defaults (base URL, API key variable, model)."""

    preset = "openai"

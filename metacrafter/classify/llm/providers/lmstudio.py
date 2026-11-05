# -*- coding: utf-8 -*-
"""LMStudioProvider: the `lmstudio` preset of the OpenAI-compatible client."""
from .openai_compatible import OpenAICompatibleProvider


class LMStudioProvider(OpenAICompatibleProvider):
    """OpenAI-compatible client with `lmstudio` defaults (base URL, API key variable, model)."""

    preset = "lmstudio"

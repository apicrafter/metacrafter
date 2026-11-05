# -*- coding: utf-8 -*-
"""PerplexityProvider: the `perplexity` preset of the OpenAI-compatible client."""
from .openai_compatible import OpenAICompatibleProvider


class PerplexityProvider(OpenAICompatibleProvider):
    """OpenAI-compatible client with `perplexity` defaults (base URL, API key variable, model)."""

    preset = "perplexity"

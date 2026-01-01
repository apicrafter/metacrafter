# -*- coding: utf-8 -*-
"""LLM-based classification for Metacrafter."""

# Conditional import to handle missing optional dependencies
try:
    from .classifier import LLMClassifier
    __all__ = ["LLMClassifier"]
except ImportError:
    # If dependencies are missing, LLMClassifier won't be available
    # This allows the package to be imported even without openai/chromadb
    __all__ = []


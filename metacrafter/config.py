# -*- coding: utf-8 -*-
"""Configuration management module for Metacrafter."""
import logging
import os
import math
from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from metacrafter.exceptions import ConfigurationError

DEFAULT_METACRAFTER_CONFIGFILE = ".metacrafter"
def discover_extended_rules_path() -> Optional[str]:
    """Return the rules directory of the installed ``metacrafter-rules`` package.

    ``metacrafter-rules`` is a required dependency and provides the default rule pack.
    Returns ``None`` when the package is missing or ships no YAML rules.
    """
    try:
        from metacrafterext.rules import rules_dir
    except ImportError:
        return None

    path = Path(rules_dir())
    if path.is_dir() and any(path.rglob("*.yaml")):
        return str(path)
    return None


def default_rulepath() -> List[str]:
    """Return the default rule pack location as a list (empty when unavailable)."""
    path = discover_extended_rules_path()
    return [path] if path else []


# Resolved once at import time for callers that need a constant. The default pack
# always comes from the installed ``metacrafter-rules`` package, never from the
# current working directory.
DEFAULT_RULEPATH: List[str] = default_rulepath()


def _normalize_rulepath_key(path: str) -> str:
    if os.path.exists(path):
        return os.path.normpath(os.path.abspath(path))
    return os.path.normpath(path)


def build_rulepath(
    user_paths: Optional[List[str]] = None,
    auto_rules: bool = True,
) -> List[str]:
    """Compose the effective rulepath.

    Order: the ``metacrafter-rules`` default pack (unless ``auto_rules`` is false),
    then user paths from ``.metacrafter`` and CLI ``--rulepath`` in the given order.
    Later entries override earlier ones for the same rule id.
    """
    ordered: List[str] = []
    seen = set()

    def add(path: str) -> None:
        key = _normalize_rulepath_key(path)
        if key in seen:
            return
        seen.add(key)
        ordered.append(path)

    if auto_rules:
        default_pack = discover_extended_rules_path()
        if default_pack:
            logging.debug("Using default rule pack from metacrafter-rules: %s", default_pack)
            add(default_pack)
        else:
            logging.warning(
                "metacrafter-rules is not installed; no default rule pack is available"
            )
    for path in user_paths or []:
        add(path)
    return ordered


class MetacrafterConfig(BaseModel):
    """Pydantic model for Metacrafter configuration validation."""

    model_config = ConfigDict(extra="ignore")

    rulepath: List[str] = Field(default_factory=list)
    auto_rules: bool = Field(
        default=True,
        description="When true, load the default rule pack from metacrafter-rules",
    )

    # Registry configuration
    registry_url: Optional[str] = Field(default=None, description="Base URL of the metacrafter-registry service")
    registry_path: Optional[str] = Field(default=None, description="Local registry snapshot or checkout data/ directory")
    registry_offline: bool = Field(default=False, description="Never fetch registry data over the network")
    registry_cache_ttl: float = Field(default=86400, ge=0, description="Seconds a cached registry snapshot is used without checking the network")
    registry_detectors: str = Field(default="stable", pattern="^(off|stable|all)$", description="Detectors derived from registry regexps: off, stable or all")

    # LLM configuration fields
    classification_mode: Optional[str] = Field(default="rules", description="Classification mode: rules, llm, or hybrid")
    llm_provider: Optional[str] = Field(default="openai", description="LLM provider: openai, openrouter, ollama, lmstudio, perplexity")
    llm_registry_path: Optional[str] = None
    llm_index_path: Optional[str] = None
    llm_model: Optional[str] = None
    llm_api_key: Optional[str] = None
    llm_base_url: Optional[str] = None
    llm_min_confidence: Optional[float] = Field(default=50.0, description="Minimum confidence for LLM results (0-100)")
    
    @field_validator('rulepath')
    @classmethod
    def validate_rulepath(cls, v):
        """Validate that all rule paths exist.
        
        Args:
            v: List of rule paths
            
        Returns:
            Validated list of rule paths
            
        Raises:
            ConfigurationError: If any path does not exist
        """
        for path in v:
            if not isinstance(path, str):
                raise ValueError(f"Rule path must be a string, got {type(path)}")
            if not os.path.exists(path):
                raise ConfigurationError(
                    f"Rule path does not exist: {path}. "
                    "Please check your configuration file."
                )
        return v
    
    @field_validator('classification_mode')
    @classmethod
    def validate_classification_mode(cls, v):
        """Validate classification mode."""
        if v is not None and v.lower() not in ("rules", "llm", "hybrid"):
            raise ValueError(f"classification_mode must be one of: rules, llm, hybrid. Got: {v}")
        return v.lower() if v else "rules"
    
    @field_validator('llm_provider')
    @classmethod
    def validate_llm_provider(cls, v):
        """Validate LLM provider."""
        if v is not None and v.lower() not in ("openai", "openrouter", "ollama", "lmstudio", "perplexity"):
            raise ValueError(f"llm_provider must be one of: openai, openrouter, ollama, lmstudio, perplexity. Got: {v}")
        return v.lower() if v else "openai"
    
    @field_validator('llm_min_confidence')
    @classmethod
    def validate_llm_min_confidence(cls, v):
        """Validate LLM minimum confidence."""
        if v is not None and (not math.isfinite(v) or v < 0 or v > 100):
            raise ValueError(f"llm_min_confidence must be between 0 and 100. Got: {v}")
        return v


class ConfigLoader:
    """Centralized configuration loader for Metacrafter."""

    @staticmethod
    def load_config() -> Optional[dict]:
        """Load configuration from .metacrafter file.
        
        Looks for configuration file in:
        1. Current directory (.metacrafter)
        2. Home directory (~/.metacrafter)
        
        Returns:
            Dictionary with configuration or None if no config found
            
        Raises:
            yaml.YAMLError: If YAML file is malformed
            IOError: If file cannot be read
        """
        filepath = None
        if os.path.exists(DEFAULT_METACRAFTER_CONFIGFILE):
            logging.debug("Local .metacrafter config exists. Using it")
            filepath = DEFAULT_METACRAFTER_CONFIGFILE
        elif os.path.exists(
            os.path.join(os.path.expanduser("~"), DEFAULT_METACRAFTER_CONFIGFILE)
        ):
            logging.debug("Home dir .metacrafter config exists. Using it")
            filepath = os.path.join(
                os.path.expanduser("~"), DEFAULT_METACRAFTER_CONFIGFILE
            )

        if filepath:
            try:
                with open(filepath, "r", encoding="utf8") as f:
                    config = yaml.safe_load(f)
                    return config
            except yaml.YAMLError as e:
                logging.error("Invalid YAML configuration (%s)", type(e).__name__)
                raise
            except IOError as e:
                logging.error("Error reading config file %s: %s", filepath, e)
                raise

        return None
    
    @staticmethod
    def get_config_file_path() -> Optional[str]:
        """Get the path to the config file being used.
        
        Returns:
            Path to .metacrafter file or None if not found
        """
        if os.path.exists(DEFAULT_METACRAFTER_CONFIGFILE):
            return os.path.abspath(DEFAULT_METACRAFTER_CONFIGFILE)
        home_config = os.path.join(os.path.expanduser("~"), DEFAULT_METACRAFTER_CONFIGFILE)
        if os.path.exists(home_config):
            return home_config
        return None
    
    @staticmethod
    def get_auto_rules() -> bool:
        """Return whether auto-discovery of metacrafter-rules is enabled."""
        config = ConfigLoader.load_config()
        if not config:
            return True
        try:
            validated_config = MetacrafterConfig(**config)
            return validated_config.auto_rules
        except Exception as e:
            if isinstance(e, ConfigurationError):
                raise
            raise ConfigurationError(
                "Invalid configuration. "
                "Please check your .metacrafter configuration file."
            ) from e

    @staticmethod
    def get_rulepath(extra_paths: Optional[List[str]] = None) -> List[str]:
        """Get composed rule paths from configuration, auto-discovery, and CLI overrides.

        Args:
            extra_paths: Optional CLI ``--rulepath`` entries appended after built-in
                and auto-discovered paths.

        Returns:
            Ordered list of rule directories to load.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        config = ConfigLoader.load_config()
        auto_rules = True
        user_paths: Optional[List[str]] = None

        if config:
            try:
                validated_config = MetacrafterConfig(**config)
                auto_rules = validated_config.auto_rules
                if "rulepath" in config:
                    user_paths = validated_config.rulepath
            except Exception as e:
                if isinstance(e, ConfigurationError):
                    raise
                raise ConfigurationError(
                    "Invalid configuration. "
                    "Please check your .metacrafter configuration file."
                ) from e

        if extra_paths:
            user_paths = (user_paths or []) + list(extra_paths)

        paths = build_rulepath(user_paths=user_paths, auto_rules=auto_rules)
        if not paths:
            raise ConfigurationError(
                "No rule directories are configured. Install metacrafter-rules, "
                "or set 'rulepath' in .metacrafter, or pass --rulepath."
            )
        return paths
    
    @staticmethod
    def get_registry_url() -> str:
        """Resolve the registry base URL.

        Precedence: ``METACRAFTER_REGISTRY_URL`` environment variable, then the
        ``registry_url`` value from ``.metacrafter`` config, then the built-in
        default (``https://registry.apicrafter.io``).

        Returns:
            The resolved registry base URL (without a trailing slash).
        """
        from metacrafter.registry.client import BASE_REGISTRY_URL

        env_url = os.environ.get("METACRAFTER_REGISTRY_URL")
        if env_url:
            return env_url.rstrip("/")

        config = ConfigLoader.load_config()
        if config:
            try:
                validated_config = MetacrafterConfig(**config)
                if validated_config.registry_url:
                    return validated_config.registry_url.rstrip("/")
            except Exception as e:  # noqa: BLE001 - fall back to default on any error
                logging.debug("Registry configuration validation failed (%s)", type(e).__name__)
        return BASE_REGISTRY_URL.rstrip("/")

    @staticmethod
    def get_registry_settings() -> dict:
        """Resolve registry adapter settings from the environment and ``.metacrafter``.

        Environment variables win: ``METACRAFTER_REGISTRY_URL``,
        ``METACRAFTER_REGISTRY_PATH`` and ``METACRAFTER_REGISTRY_OFFLINE`` (1/true/yes).
        """
        settings = {
            "base_url": ConfigLoader.get_registry_url(),
            "registry_path": None,
            "offline": False,
            "cache_ttl": 86400.0,
        }
        config = ConfigLoader.load_config()
        if config:
            try:
                validated = MetacrafterConfig(**config)
                settings["registry_path"] = validated.registry_path
                settings["offline"] = validated.registry_offline
                settings["cache_ttl"] = validated.registry_cache_ttl
                settings["detectors"] = validated.registry_detectors
            except Exception as e:  # noqa: BLE001 - invalid config falls back to defaults
                logging.debug("Registry configuration validation failed (%s)", type(e).__name__)
        env_path = os.environ.get("METACRAFTER_REGISTRY_PATH")
        if env_path:
            settings["registry_path"] = env_path
        env_offline = os.environ.get("METACRAFTER_REGISTRY_OFFLINE")
        if env_offline is not None:
            settings["offline"] = env_offline.strip().lower() in ("1", "true", "yes", "on")
        return settings

    @staticmethod
    def get_llm_config() -> Optional[dict]:
        """Get LLM configuration from config file.
        
        Returns:
            Dictionary with LLM configuration or None if not configured
        """
        config = ConfigLoader.load_config()
        if config:
            try:
                validated_config = MetacrafterConfig(**config)
                llm_config = {
                    "classification_mode": validated_config.classification_mode,
                    "llm_provider": validated_config.llm_provider,
                    "llm_registry_path": validated_config.llm_registry_path,
                    "llm_index_path": validated_config.llm_index_path,
                    "llm_model": validated_config.llm_model,
                    "llm_api_key": validated_config.llm_api_key,
                    "llm_base_url": validated_config.llm_base_url,
                    "llm_min_confidence": validated_config.llm_min_confidence,
                }
                # Check if any LLM-specific setting is configured (excluding defaults)
                # Consider it configured if:
                # 1. classification_mode is not "rules" (default)
                # 2. llm_provider is not "openai" (default)
                # 3. Any other LLM setting is not None
                has_llm_config = (
                    llm_config.get("classification_mode") not in (None, "rules") or
                    llm_config.get("llm_provider") not in (None, "openai") or
                    llm_config.get("llm_registry_path") is not None or
                    llm_config.get("llm_index_path") is not None or
                    llm_config.get("llm_model") is not None or
                    llm_config.get("llm_api_key") is not None or
                    llm_config.get("llm_base_url") is not None or
                    llm_config.get("llm_min_confidence") not in (None, 50.0)
                )
                
                if has_llm_config:
                    logging.debug("Loaded LLM config (credentials redacted)")
                    return llm_config
                else:
                    logging.debug("No LLM configuration found in config file")
            except Exception as e:
                logging.warning("LLM configuration validation failed")
                logging.debug("Configuration validation failed; contents omitted")
        else:
            logging.debug("No .metacrafter config file found")
        return None
    
    @staticmethod
    def get_classification_mode() -> str:
        """Get classification mode from config.
        
        Returns:
            Classification mode: "rules", "llm", or "hybrid" (defaults to "rules")
        """
        llm_config = ConfigLoader.get_llm_config()
        if llm_config and llm_config.get("classification_mode"):
            return llm_config["classification_mode"]
        return "rules"
    
    @staticmethod
    def get_llm_provider() -> str:
        """Get LLM provider from config.
        
        Returns:
            Provider name (defaults to "openai")
        """
        llm_config = ConfigLoader.get_llm_config()
        if llm_config and llm_config.get("llm_provider"):
            return llm_config["llm_provider"]
        return "openai"


# -*- coding: utf-8 -*-
"""Configuration management module for Metacrafter."""
import logging
import os
from typing import List, Optional

import yaml
from pydantic import BaseModel, validator, Field

from metacrafter.exceptions import ConfigurationError

DEFAULT_METACRAFTER_CONFIGFILE = ".metacrafter"
DEFAULT_RULEPATH = ["rules"]


class MetacrafterConfig(BaseModel):
    """Pydantic model for Metacrafter configuration validation."""
    
    rulepath: List[str] = Field(default_factory=lambda: DEFAULT_RULEPATH.copy())
    
    # LLM configuration fields
    classification_mode: Optional[str] = Field(default="rules", description="Classification mode: rules, llm, or hybrid")
    llm_provider: Optional[str] = Field(default="openai", description="LLM provider: openai, openrouter, ollama, lmstudio, perplexity")
    llm_registry_path: Optional[str] = None
    llm_index_path: Optional[str] = None
    llm_model: Optional[str] = None
    llm_api_key: Optional[str] = None
    llm_base_url: Optional[str] = None
    llm_min_confidence: Optional[float] = Field(default=50.0, description="Minimum confidence for LLM results (0-100)")
    
    @validator('rulepath')
    def validate_rulepath(cls, v):
        """Validate that all rule paths exist.
        
        Args:
            v: List of rule paths
            
        Returns:
            Validated list of rule paths
            
        Raises:
            ConfigurationError: If any path does not exist
        """
        if not v:
            raise ValueError("rulepath cannot be empty")
        for path in v:
            if not isinstance(path, str):
                raise ValueError(f"Rule path must be a string, got {type(path)}")
            if not os.path.exists(path):
                raise ConfigurationError(
                    f"Rule path does not exist: {path}. "
                    "Please check your configuration file."
                )
        return v
    
    @validator('classification_mode')
    def validate_classification_mode(cls, v):
        """Validate classification mode."""
        if v is not None and v.lower() not in ("rules", "llm", "hybrid"):
            raise ValueError(f"classification_mode must be one of: rules, llm, hybrid. Got: {v}")
        return v.lower() if v else "rules"
    
    @validator('llm_provider')
    def validate_llm_provider(cls, v):
        """Validate LLM provider."""
        if v is not None and v.lower() not in ("openai", "openrouter", "ollama", "lmstudio", "perplexity"):
            raise ValueError(f"llm_provider must be one of: openai, openrouter, ollama, lmstudio, perplexity. Got: {v}")
        return v.lower() if v else "openai"
    
    @validator('llm_min_confidence')
    def validate_llm_min_confidence(cls, v):
        """Validate LLM minimum confidence."""
        if v is not None and (v < 0 or v > 100):
            raise ValueError(f"llm_min_confidence must be between 0 and 100. Got: {v}")
        return v
    
    class Config:
        """Pydantic configuration."""
        extra = "ignore"  # Ignore extra fields in config file


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
                logging.error("Error parsing YAML config file %s: %s", filepath, e)
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
    def get_rulepath() -> List[str]:
        """Get rule path from configuration with validation.
        
        Returns:
            List of rule paths, defaults to ["rules"] if no config found
            
        Raises:
            ConfigurationError: If configuration is invalid
        """
        config = ConfigLoader.load_config()
        if config:
            try:
                validated_config = MetacrafterConfig(**config)
                return validated_config.rulepath
            except Exception as e:
                if isinstance(e, ConfigurationError):
                    raise
                raise ConfigurationError(
                    f"Invalid configuration: {e}. "
                    "Please check your .metacrafter configuration file."
                ) from e
        return DEFAULT_RULEPATH
    
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
                    logging.debug(f"Loaded LLM config: {llm_config}")
                    return llm_config
                else:
                    logging.debug("No LLM configuration found in config file")
            except Exception as e:
                logging.warning(f"Error loading LLM config: {e}")
                logging.debug(f"Config content: {config}", exc_info=True)
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


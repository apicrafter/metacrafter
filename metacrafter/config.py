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


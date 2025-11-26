# -*- coding: utf-8 -*-
"""Configuration management module for Metacrafter."""
import logging
import os
from typing import List, Optional

import yaml

DEFAULT_METACRAFTER_CONFIGFILE = ".metacrafter"
DEFAULT_RULEPATH = ["rules"]


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
                logging.error(f"Error parsing YAML config file {filepath}: {e}")
                raise
            except IOError as e:
                logging.error(f"Error reading config file {filepath}: {e}")
                raise
        
        return None

    @staticmethod
    def get_rulepath() -> List[str]:
        """Get rule path from configuration.
        
        Returns:
            List of rule paths, defaults to ["rules"] if no config found
        """
        config = ConfigLoader.load_config()
        if config and "rulepath" in config:
            return config["rulepath"]
        return DEFAULT_RULEPATH


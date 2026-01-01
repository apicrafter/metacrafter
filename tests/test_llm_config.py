# -*- coding: utf-8 -*-
"""Tests for LLM configuration support."""
import pytest
import os
import tempfile
import yaml
from metacrafter.config import ConfigLoader, MetacrafterConfig


class TestLLMConfig:
    """Tests for LLM configuration."""
    
    def test_metacrafter_config_llm_fields(self):
        """Test MetacrafterConfig includes LLM fields."""
        config = MetacrafterConfig(
            rulepath=["rules"],
            classification_mode="hybrid",
            llm_provider="openai",
            llm_model="gpt-4o-mini",
            llm_min_confidence=60.0
        )
        
        assert config.classification_mode == "hybrid"
        assert config.llm_provider == "openai"
        assert config.llm_model == "gpt-4o-mini"
        assert config.llm_min_confidence == 60.0
    
    def test_metacrafter_config_llm_defaults(self):
        """Test MetacrafterConfig LLM field defaults."""
        config = MetacrafterConfig(rulepath=["rules"])
        
        assert config.classification_mode == "rules"
        assert config.llm_provider == "openai"
        assert config.llm_min_confidence == 50.0
    
    def test_metacrafter_config_validate_classification_mode(self):
        """Test validation of classification_mode."""
        with pytest.raises(ValueError, match="classification_mode must be one of"):
            MetacrafterConfig(
                rulepath=["rules"],
                classification_mode="invalid_mode"
            )
    
    def test_metacrafter_config_validate_llm_provider(self):
        """Test validation of llm_provider."""
        with pytest.raises(ValueError, match="llm_provider must be one of"):
            MetacrafterConfig(
                rulepath=["rules"],
                llm_provider="invalid_provider"
            )
    
    def test_metacrafter_config_validate_llm_min_confidence(self):
        """Test validation of llm_min_confidence."""
        with pytest.raises(ValueError, match="llm_min_confidence must be between"):
            MetacrafterConfig(
                rulepath=["rules"],
                llm_min_confidence=150.0  # Invalid: > 100
            )
        
        with pytest.raises(ValueError, match="llm_min_confidence must be between"):
            MetacrafterConfig(
                rulepath=["rules"],
                llm_min_confidence=-10.0  # Invalid: < 0
            )
    
    def test_config_loader_get_llm_config(self):
        """Test loading LLM config from file."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.metacrafter', delete=False
        ) as f:
            config = {
                "rulepath": ["rules"],
                "classification_mode": "hybrid",
                "llm_provider": "openai",
                "llm_model": "gpt-4o-mini",
                "llm_registry_path": "../registry/data/datatypes_latest.jsonl",
                "llm_index_path": "./llm_index",
                "llm_min_confidence": 60.0
            }
            yaml.dump(config, f)
            config_path = f.name
        
        try:
            original_dir = os.getcwd()
            config_dir = os.path.dirname(config_path)
            os.chdir(config_dir)
            config_filename = os.path.basename(config_path)
            
            # Temporarily rename if .metacrafter exists
            original_config = None
            if os.path.exists(".metacrafter"):
                original_config = ".metacrafter.backup"
                os.rename(".metacrafter", original_config)
            
            try:
                os.rename(config_filename, ".metacrafter")
                llm_config = ConfigLoader.get_llm_config()
                
                assert llm_config is not None
                assert llm_config["classification_mode"] == "hybrid"
                assert llm_config["llm_provider"] == "openai"
                assert llm_config["llm_model"] == "gpt-4o-mini"
                assert llm_config["llm_min_confidence"] == 60.0
            finally:
                if os.path.exists(".metacrafter"):
                    os.remove(".metacrafter")
                if original_config and os.path.exists(original_config):
                    os.rename(original_config, ".metacrafter")
                os.chdir(original_dir)
        finally:
            if os.path.exists(config_path):
                os.remove(config_path)
    
    def test_config_loader_get_llm_config_no_config(self):
        """Test get_llm_config returns None when no config."""
        # Ensure we're not in a directory with .metacrafter
        original_dir = os.getcwd()
        temp_dir = tempfile.mkdtemp()
        
        try:
            os.chdir(temp_dir)
            # Remove any existing .metacrafter
            if os.path.exists(".metacrafter"):
                os.remove(".metacrafter")
            
            llm_config = ConfigLoader.get_llm_config()
            assert llm_config is None
        finally:
            os.chdir(original_dir)
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_config_loader_get_classification_mode(self):
        """Test get_classification_mode method."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.metacrafter', delete=False
        ) as f:
            config = {
                "rulepath": ["rules"],
                "classification_mode": "llm"
            }
            yaml.dump(config, f)
            config_path = f.name
        
        try:
            original_dir = os.getcwd()
            config_dir = os.path.dirname(config_path)
            os.chdir(config_dir)
            config_filename = os.path.basename(config_path)
            
            original_config = None
            if os.path.exists(".metacrafter"):
                original_config = ".metacrafter.backup"
                os.rename(".metacrafter", original_config)
            
            try:
                os.rename(config_filename, ".metacrafter")
                mode = ConfigLoader.get_classification_mode()
                assert mode == "llm"
            finally:
                if os.path.exists(".metacrafter"):
                    os.remove(".metacrafter")
                if original_config and os.path.exists(original_config):
                    os.rename(original_config, ".metacrafter")
                os.chdir(original_dir)
        finally:
            if os.path.exists(config_path):
                os.remove(config_path)
    
    def test_config_loader_get_classification_mode_default(self):
        """Test get_classification_mode returns default when not configured."""
        original_dir = os.getcwd()
        temp_dir = tempfile.mkdtemp()
        
        try:
            os.chdir(temp_dir)
            if os.path.exists(".metacrafter"):
                os.remove(".metacrafter")
            
            mode = ConfigLoader.get_classification_mode()
            assert mode == "rules"  # Default
        finally:
            os.chdir(original_dir)
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_config_loader_get_llm_provider(self):
        """Test get_llm_provider method."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.metacrafter', delete=False
        ) as f:
            config = {
                "rulepath": ["rules"],
                "llm_provider": "ollama"
            }
            yaml.dump(config, f)
            config_path = f.name
        
        try:
            original_dir = os.getcwd()
            config_dir = os.path.dirname(config_path)
            os.chdir(config_dir)
            config_filename = os.path.basename(config_path)
            
            original_config = None
            if os.path.exists(".metacrafter"):
                original_config = ".metacrafter.backup"
                os.rename(".metacrafter", original_config)
            
            try:
                os.rename(config_filename, ".metacrafter")
                provider = ConfigLoader.get_llm_provider()
                assert provider == "ollama"
            finally:
                if os.path.exists(".metacrafter"):
                    os.remove(".metacrafter")
                if original_config and os.path.exists(original_config):
                    os.rename(original_config, ".metacrafter")
                os.chdir(original_dir)
        finally:
            if os.path.exists(config_path):
                os.remove(config_path)
    
    def test_config_loader_get_llm_provider_default(self):
        """Test get_llm_provider returns default when not configured."""
        original_dir = os.getcwd()
        temp_dir = tempfile.mkdtemp()
        
        try:
            os.chdir(temp_dir)
            if os.path.exists(".metacrafter"):
                os.remove(".metacrafter")
            
            provider = ConfigLoader.get_llm_provider()
            assert provider == "openai"  # Default
        finally:
            os.chdir(original_dir)
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)


"""Tests for error handling and edge cases."""
import pytest
import os
import tempfile
from metacrafter.core import CrafterCmd
from metacrafter.exceptions import ConfigurationError, FileProcessingError


def test_scan_file_nonexistent():
    """Test scanning non-existent file."""
    cmd = CrafterCmd(remote=None, debug=False)
    result = cmd.scan_file("/nonexistent/file.csv")
    assert result is None or result == []


def test_scan_file_invalid_format():
    """Test scanning unsupported file format."""
    cmd = CrafterCmd(remote=None, debug=False)
    with tempfile.NamedTemporaryFile(suffix=".unknown", delete=False) as f:
        f.write(b"test data")
        temp_path = f.name
    
    try:
        result = cmd.scan_file(temp_path)
        # Should handle gracefully, not raise exception
        assert result is None or result == []
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def test_scan_file_empty_file():
    """Test scanning empty file."""
    cmd = CrafterCmd(remote=None, debug=False)
    with tempfile.NamedTemporaryFile(mode='w', suffix=".csv", delete=False) as f:
        temp_path = f.name
    
    try:
        result = cmd.scan_file(temp_path)
        # Should handle empty file gracefully
        assert result["report_version"] == 3 and result["data"] == []
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def test_config_validation_invalid_path():
    """Test configuration validation with invalid path."""
    from metacrafter.config import MetacrafterConfig
    
    with pytest.raises(ConfigurationError, match="Rule path does not exist"):
        MetacrafterConfig(rulepath=["/nonexistent/path"])


def test_config_validation_empty_rulepath():
    """An empty user rulepath is valid: the default pack still applies."""
    from metacrafter.config import MetacrafterConfig

    assert MetacrafterConfig(rulepath=[]).rulepath == []


def test_config_validation_valid_path(tmp_path):
    """Test configuration validation with valid path."""
    from metacrafter.config import MetacrafterConfig
    
    # Create a temporary directory
    rule_dir = tmp_path / "rules"
    rule_dir.mkdir()
    
    config = MetacrafterConfig(rulepath=[str(rule_dir)])
    assert config.rulepath == [str(rule_dir)]


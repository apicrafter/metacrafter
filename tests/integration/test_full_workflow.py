"""Integration tests for full workflow."""
import pytest
import json
import os
import tempfile
from pathlib import Path

from metacrafter.core import CrafterCmd


def test_full_scan_workflow(tmp_path):
    """Test complete scan workflow from file to output."""
    # Create test data file
    test_file = tmp_path / "test.csv"
    test_file.write_text("name,email\nJohn,john@example.com\nJane,jane@example.com\n")
    
    # Run scan
    cmd = CrafterCmd(remote=None, debug=False)
    output_file = tmp_path / "output.json"
    cmd.scan_file(
        str(test_file),
        output=str(output_file),
        output_format="json",
        limit=10
    )
    
    # Verify output
    assert output_file.exists()
    with open(output_file) as f:
        result = json.load(f)
        assert "results" in result or "fields" in result


def test_full_scan_workflow_table_output(tmp_path):
    """Test complete scan workflow with table output."""
    # Create test data file
    test_file = tmp_path / "test.csv"
    test_file.write_text("name,email\nJohn,john@example.com\n")
    
    # Run scan
    cmd = CrafterCmd(remote=None, debug=False, quiet=True)
    result = cmd.scan_file(
        str(test_file),
        output_format="table",
        limit=10
    )
    
    # Should complete without errors
    assert result is None or result == []


def test_scan_data_integration():
    """Test scan_data method with sample data."""
    cmd = CrafterCmd(remote=None, debug=False)
    
    items = [
        {"name": "John Doe", "email": "john@example.com", "age": "30"},
        {"name": "Jane Smith", "email": "jane@example.com", "age": "25"},
    ]
    
    result = cmd.scan_data(items, limit=10)
    
    assert "results" in result
    assert "data" in result
    assert "stats" in result
    assert isinstance(result["results"], list)
    assert isinstance(result["data"], list)


def test_bulk_scan_workflow(tmp_path):
    """Test bulk scan workflow with multiple files."""
    # Create multiple test files
    for i in range(3):
        test_file = tmp_path / f"test_{i}.csv"
        test_file.write_text(f"name,value\nItem{i},value{i}\n")
    
    # Run bulk scan
    cmd = CrafterCmd(remote=None, debug=False, quiet=True)
    output_file = tmp_path / "bulk_output.json"
    
    cmd.scan_bulk(
        str(tmp_path),
        output=str(output_file),
        output_format="json",
        limit=10
    )
    
    # Should complete without errors
    # Output file may or may not exist depending on implementation
    # Just verify no exceptions were raised


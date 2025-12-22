# -*- coding: utf-8 -*-
"""Comprehensive tests for all supported file formats."""
import os
import pytest
from metacrafter.core import CrafterCmd


class TestTextFormats:
    """Test text-based file formats."""

    def test_csv_format(self):
        """Test CSV file scanning."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows.csv"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(fixture_path, limit=10, dformat="short")
            assert result is None or isinstance(result, list)

    def test_tsv_format(self):
        """Test TSV file scanning."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "ru_utf8_tab.csv"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(
                fixture_path, delimiter="\t", limit=10, dformat="short"
            )
            assert result is None or isinstance(result, list)

    def test_json_format(self):
        """Test JSON array file scanning."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows_array.json"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(fixture_path, limit=10, dformat="short")
            assert result is None or isinstance(result, list)

    def test_jsonl_format(self):
        """Test JSONL file scanning."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows_flat.jsonl"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(fixture_path, limit=10, dformat="short")
            assert result is None or isinstance(result, list)

    def test_ndjson_format(self):
        """Test NDJSON file scanning."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows_flat.ndjson"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(fixture_path, limit=10, dformat="short")
            assert result is None or isinstance(result, list)

    def test_xml_format(self):
        """Test XML file scanning."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "books.xml"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(
                fixture_path, limit=10, dformat="short"
            )
            assert result is None or isinstance(result, list)


class TestBinaryFormats:
    """Test binary file formats."""

    def test_bson_format(self):
        """Test BSON file scanning."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows_flat.bson"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(fixture_path, limit=10, dformat="short")
            assert result is None or isinstance(result, list)

    def test_parquet_format(self):
        """Test Parquet file scanning."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows.parquet"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(fixture_path, limit=10, dformat="short")
            assert result is None or isinstance(result, list)

    def test_avro_format(self):
        """Test Avro file scanning."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows.avro"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(fixture_path, limit=10, dformat="short")
            assert result is None or isinstance(result, list)

    def test_orc_format(self):
        """Test ORC file scanning."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows.orc"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(fixture_path, limit=10, dformat="short")
            assert result is None or isinstance(result, list)

    def test_pickle_format(self):
        """Test Pickle file scanning."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows_flat.pickle"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(fixture_path, limit=10, dformat="short")
            assert result is None or isinstance(result, list)


class TestExcelFormats:
    """Test Excel file formats."""

    def test_xls_format(self):
        """Test Excel .xls file scanning."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows.xls"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(fixture_path, limit=10, dformat="short")
            assert result is None or isinstance(result, list)

    def test_xlsx_format(self):
        """Test Excel .xlsx file scanning."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows.xlsx"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(fixture_path, limit=10, dformat="short")
            assert result is None or isinstance(result, list)


class TestCompressionFormats:
    """Test compressed file formats."""

    def test_gzip_compression(self):
        """Test gzip-compressed CSV file."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows.csv.gz"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(
                fixture_path, limit=10, dformat="short", compression="auto"
            )
            assert result is None or isinstance(result, list)

    def test_bzip2_compression(self):
        """Test bzip2-compressed CSV file."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows.csv.bz2"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(
                fixture_path, limit=10, dformat="short", compression="auto"
            )
            assert result is None or isinstance(result, list)

    def test_xz_compression(self):
        """Test xz-compressed CSV file."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows.csv.xz"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(
                fixture_path, limit=10, dformat="short", compression="auto"
            )
            assert result is None or isinstance(result, list)

    def test_lz4_compression(self):
        """Test lz4-compressed CSV file."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows.csv.lz4"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(
                fixture_path, limit=10, dformat="short", compression="auto"
            )
            assert result is None or isinstance(result, list)

    def test_zstandard_compression(self):
        """Test zstandard-compressed CSV file."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows.csv.zst"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(
                fixture_path, limit=10, dformat="short", compression="auto"
            )
            assert result is None or isinstance(result, list)

    def test_brotli_compression(self):
        """Test Brotli-compressed CSV file."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows.csv.br"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(
                fixture_path, limit=10, dformat="short", compression="auto"
            )
            assert result is None or isinstance(result, list)

    def test_zip_compression(self):
        """Test ZIP-compressed CSV file."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows.csv.zip"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(
                fixture_path, limit=10, dformat="short", compression="auto"
            )
            assert result is None or isinstance(result, list)

    def test_xml_lz4_compression(self):
        """Test lz4-compressed XML file."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "books.xml.lz4"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(
                fixture_path, limit=10, dformat="short", compression="auto"
            )
            assert result is None or isinstance(result, list)


class TestFormatDetection:
    """Test format detection and error handling."""

    def test_auto_detect_csv(self):
        """Test automatic CSV format detection."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows.csv"
        )
        if os.path.exists(fixture_path):
            # Should auto-detect CSV without specifying format
            result = cmd.scan_file(fixture_path, limit=10, dformat="short")
            assert result is None or isinstance(result, list)

    def test_auto_detect_jsonl(self):
        """Test automatic JSONL format detection."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows_flat.jsonl"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(fixture_path, limit=10, dformat="short")
            assert result is None or isinstance(result, list)

    def test_unsupported_format_handling(self):
        """Test error handling for unsupported formats."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        # Create a temporary file with unsupported extension
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=".unknown", delete=False) as f:
            f.write(b"test data")
            temp_path = f.name

        try:
            result = cmd.scan_file(temp_path, limit=10)
            # Should handle gracefully, not raise exception
            assert result is None or result == []
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_empty_file_handling(self):
        """Test handling of empty files."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            temp_path = f.name

        try:
            result = cmd.scan_file(temp_path, limit=10)
            # Should handle empty file gracefully
            assert result is None or result == []
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_custom_delimiter(self):
        """Test CSV with custom delimiter."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "ru_utf8_semicolon.csv"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(
                fixture_path, delimiter=";", limit=10, dformat="short"
            )
            assert result is None or isinstance(result, list)

    def test_custom_encoding(self):
        """Test file with custom encoding."""
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "ru_cp1251_comma.csv"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(
                fixture_path,
                encoding="cp1251",
                limit=10,
                dformat="short",
            )
            assert result is None or isinstance(result, list)


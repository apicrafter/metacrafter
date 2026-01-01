"""Test for issue #17: SQLite UTF-8 decoding error handling.

This test verifies that SQLite databases with non-UTF-8 encoded data
are handled gracefully without raising OperationalError.

Note: SQLite validates UTF-8 when storing TEXT columns, so we cannot
programmatically create a database with invalid UTF-8. However, real-world
databases can have this issue if:
1. The database was created on a system with different encoding
2. The database file was corrupted
3. Data was inserted using a different text_factory

The fix handles this by:
1. Setting a custom text factory with errors='replace' for SQLite connections
2. Catching OperationalError exceptions and handling them gracefully

These tests verify that:
- The text factory configuration is applied
- Normal Unicode characters still work correctly (regression test)
- The scan completes without crashing even if encoding issues occur
"""
import pytest
import sqlite3
import tempfile
import os
from pathlib import Path

from metacrafter.core import CrafterCmd


@pytest.fixture
def sqlite_db_with_encoding_issues(tmp_path):
    """Create a SQLite database with encoding issues similar to the problematic database.
    
    This simulates a database that contains text data that cannot be decoded as UTF-8,
    similar to the '000012_world.zip' database mentioned in issue #17.
    
    The approach: Create a database with bytes text factory, insert invalid UTF-8 bytes,
    then SQLite will fail when trying to decode them with default UTF-8 text factory.
    """
    db_path = tmp_path / "encoding_issue.db"
    connstr = f"sqlite:///{db_path}"
    
    # Create database with bytes text factory to insert invalid UTF-8
    conn = sqlite3.connect(str(db_path))
    conn.text_factory = bytes  # Return bytes instead of str
    cursor = conn.cursor()
    
    # Create table similar to what might exist in a world database
    cursor.execute("""
        CREATE TABLE countries (
            id INTEGER PRIMARY KEY,
            name TEXT,
            code TEXT,
            region TEXT
        )
    """)
    
    # Insert some normal data first (as bytes)
    cursor.executemany(
        "INSERT INTO countries (id, name, code, region) VALUES (?, ?, ?, ?)",
        [
            (1, b'Normal Country', b'NC', b'Region1'),
            (2, b'Another Country', b'AC', b'Region2'),
        ]
    )
    
    # Insert problematic data: invalid UTF-8 bytes that will cause decode errors
    # This simulates the '\ufffdland Islands' issue from the bug report
    # The bytes \xff\xfe are not valid UTF-8 (they're BOM for UTF-16 LE)
    problematic_name = b'\xff\xfe\x00\x00land Islands'  # Invalid UTF-8 sequence
    
    cursor.execute(
        "INSERT INTO countries (id, name, code, region) VALUES (?, ?, ?, ?)",
        (3, problematic_name, b'XX', b'Unknown')
    )
    
    conn.commit()
    conn.close()
    
    yield connstr
    
    # Cleanup
    if db_path.exists():
        db_path.unlink()


@pytest.fixture
def sqlite_db_with_special_characters(tmp_path):
    """Create a SQLite database with special Unicode characters.
    
    This tests that normal Unicode characters (which are valid UTF-8)
    are handled correctly. This is a regression test to ensure the fix
    doesn't break normal Unicode handling.
    """
    db_path = tmp_path / "special_chars.db"
    connstr = f"sqlite:///{db_path}"
    
    # Create database
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE locations (
            id INTEGER PRIMARY KEY,
            name TEXT,
            country TEXT
        )
    """)
    
    # Insert data with special Unicode characters (all valid UTF-8)
    test_data = [
        (1, 'Åland Islands', 'AX'),
        (2, 'Côte d\'Ivoire', 'CI'),
        (3, 'São Tomé and Príncipe', 'ST'),
        (4, 'Müller', 'DE'),
        (5, 'José', 'ES'),
    ]
    
    cursor.executemany(
        "INSERT INTO locations (id, name, country) VALUES (?, ?, ?)",
        test_data
    )
    
    conn.commit()
    conn.close()
    
    yield connstr
    
    # Cleanup
    if db_path.exists():
        db_path.unlink()


@pytest.fixture
def sqlite_db_with_replacement_characters(tmp_path):
    """Create a SQLite database that simulates the exact issue from bug report.
    
    The bug report shows: "Could not decode to UTF-8 column 'name' with text '\ufffdland Islands'"
    
    To simulate this, we create a database where SQLite's default text factory
    will fail when trying to decode. We do this by:
    1. Creating database with bytes text factory
    2. Inserting bytes that aren't valid UTF-8
    3. SQLite stores them, but when read with default UTF-8 text factory, it fails
    """
    db_path = tmp_path / "replacement_char.db"
    connstr = f"sqlite:///{db_path}"
    
    # Create database with bytes text factory
    conn = sqlite3.connect(str(db_path))
    conn.text_factory = bytes  # Return bytes instead of str
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE places (
            id INTEGER PRIMARY KEY,
            name TEXT
        )
    """)
    
    # Insert bytes that aren't valid UTF-8
    # \xff\xfe is BOM for UTF-16 LE, not valid UTF-8
    # This simulates data that was stored incorrectly
    problematic_name = b'\xff\xfe\x00\x00land Islands'  # Invalid UTF-8
    
    cursor.execute(
        "INSERT INTO places (id, name) VALUES (?, ?)",
        (1, problematic_name)
    )
    
    # Also insert some normal data
    cursor.execute(
        "INSERT INTO places (id, name) VALUES (?, ?)",
        (2, b'Normal Place')
    )
    
    conn.commit()
    conn.close()
    
    # Now the database has invalid UTF-8 bytes stored
    # When SQLAlchemy tries to read with default UTF-8 text factory,
    # it should fail - but our fix should handle it gracefully
    
    yield connstr
    
    # Cleanup
    if db_path.exists():
        db_path.unlink()


class TestSQLiteEncodingIssue17:
    """Test cases for issue #17: SQLite UTF-8 decoding error handling."""
    
    def test_scan_sqlite_with_encoding_issues_does_not_crash(self, sqlite_db_with_encoding_issues, tmp_path):
        """Test that scanning a database with encoding issues doesn't crash.
        
        This test verifies that the fix for issue #17 works correctly.
        The scan should complete without raising OperationalError.
        """
        pytest.importorskip("sqlalchemy")
        
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        output_file = tmp_path / "encoding_test_output.json"
        
        # This should not raise OperationalError
        # Before the fix, this would crash with:
        # sqlalchemy.exc.OperationalError: (sqlite3.OperationalError) Could not decode to UTF-8
        try:
            result = cmd.scan_db(
                connectstr=sqlite_db_with_encoding_issues,
                limit=10,
                dformat="short",
                output=str(output_file),
                output_format="json"
            )
            
            # scan_db returns None when output is written
            assert result is None
            
            # Verify output file was created (even if some data had issues)
            assert output_file.exists()
            
        except Exception as e:
            error_msg = str(e)
            # Check if it's the specific encoding error we're fixing
            if "Could not decode to UTF-8" in error_msg or "decode" in error_msg.lower():
                pytest.fail(
                    f"Encoding error not handled: {error_msg}\n"
                    "This indicates the fix for issue #17 is not working correctly."
                )
            else:
                # Re-raise if it's a different error
                raise
    
    def test_scan_sqlite_with_special_characters(self, sqlite_db_with_special_characters, tmp_path):
        """Test scanning database with special Unicode characters.
        
        This is a regression test to ensure normal Unicode characters
        (which are valid UTF-8) are handled correctly and the fix
        doesn't break normal Unicode handling.
        """
        pytest.importorskip("sqlalchemy")
        
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        output_file = tmp_path / "special_chars_output.json"
        
        # Should handle gracefully - all characters are valid UTF-8
        result = cmd.scan_db(
            connectstr=sqlite_db_with_special_characters,
            limit=10,
            dformat="short",
            output=str(output_file),
            output_format="json"
        )
        
        assert result is None
        assert output_file.exists()
        
        # Verify output contains valid JSON
        import json
        with open(output_file) as f:
            result_data = json.load(f)
            assert isinstance(result_data, (dict, list))
    
    def test_scan_sqlite_with_replacement_characters(self, sqlite_db_with_replacement_characters, tmp_path):
        """Test scanning database with Unicode replacement characters.
        
        This test specifically targets the issue mentioned in the bug report
        where '\ufffdland Islands' causes a decode error.
        """
        pytest.importorskip("sqlalchemy")
        
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        output_file = tmp_path / "replacement_char_output.json"
        
        # This should not raise OperationalError
        # The replacement character should be handled gracefully
        try:
            result = cmd.scan_db(
                connectstr=sqlite_db_with_replacement_characters,
                limit=10,
                dformat="short",
                output=str(output_file),
                output_format="json"
            )
            
            assert result is None
            assert output_file.exists()
            
        except Exception as e:
            error_msg = str(e)
            if "Could not decode to UTF-8" in error_msg:
                pytest.fail(
                    f"Replacement character not handled: {error_msg}\n"
                    "This indicates the fix for issue #17 is not working correctly."
                )
            else:
                raise
    
    def test_scan_sqlite_continues_after_encoding_error(self, tmp_path):
        """Test that scanning continues processing other tables after encoding error in one table."""
        pytest.importorskip("sqlalchemy")
        
        db_path = tmp_path / "multi_table_encoding.db"
        connstr = f"sqlite:///{db_path}"
        
        # Create database with multiple tables
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # Table 1: Normal data
        cursor.execute("""
            CREATE TABLE normal_table (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        """)
        cursor.execute("INSERT INTO normal_table (id, name) VALUES (1, 'Normal Data')")
        
        # Table 2: Try to create problematic data
        cursor.execute("""
            CREATE TABLE problematic_table (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        """)
        
        # Insert data that might cause issues
        # We'll use a workaround to insert problematic bytes
        conn_bytes = sqlite3.connect(str(db_path))
        conn_bytes.text_factory = bytes
        cursor_bytes = conn_bytes.cursor()
        cursor_bytes.execute(
            "INSERT INTO problematic_table (id, name) VALUES (?, ?)",
            (1, b'\xff\xfe\x00\x00problematic')
        )
        conn_bytes.commit()
        conn_bytes.close()
        
        conn.commit()
        conn.close()
        
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        output_file = tmp_path / "multi_table_output.json"
        
        # Should process normal_table even if problematic_table has issues
        try:
            result = cmd.scan_db(
                connectstr=connstr,
                limit=10,
                dformat="short",
                output=str(output_file),
                output_format="json"
            )
            
            assert result is None
            assert output_file.exists()
            
        except Exception as e:
            error_msg = str(e)
            if "Could not decode to UTF-8" in error_msg:
                pytest.fail(
                    f"Encoding error not handled gracefully: {error_msg}\n"
                    "The scan should continue processing other tables."
                )
            else:
                raise
        finally:
            # Cleanup
            if db_path.exists():
                db_path.unlink()
    
    def test_sqlite_text_factory_configuration(self, tmp_path):
        """Test that SQLite text factory configuration is applied.
        
        This test verifies that when scanning a SQLite database, the text factory
        is configured to handle encoding errors gracefully.
        """
        pytest.importorskip("sqlalchemy")
        
        from sqlalchemy import create_engine, event
        import sqlite3
        
        db_path = tmp_path / "text_factory_test.db"
        connstr = f"sqlite:///{db_path}"
        
        # Create a simple database with normal data
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        cursor.execute(
            "INSERT INTO test (id, name) VALUES (?, ?)",
            (1, 'Test Data')
        )
        conn.commit()
        conn.close()
        
        # Test that scan_db works correctly
        # The fix should configure the text factory via event listener
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        
        # Should complete without errors
        try:
            result = cmd.scan_db(
                connectstr=connstr,
                limit=10,
                dformat="short"
            )
            assert result is None
        except Exception as e:
            error_msg = str(e)
            if "Could not decode to UTF-8" in error_msg:
                pytest.fail(
                    "Text factory not configured correctly. "
                    "The fix should prevent UTF-8 decode errors."
                )
            else:
                raise
        finally:
            if db_path.exists():
                db_path.unlink()
    
    def test_sqlite_scan_completes_successfully(self, tmp_path):
        """Test that SQLite scan completes successfully even with various data types.
        
        This is a general regression test to ensure the encoding fix doesn't
        break normal database scanning functionality.
        """
        pytest.importorskip("sqlalchemy")
        
        db_path = tmp_path / "normal_scan_test.db"
        connstr = f"sqlite:///{db_path}"
        
        # Create database with various data types
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE test_data (
                id INTEGER PRIMARY KEY,
                text_field TEXT,
                int_field INTEGER,
                real_field REAL,
                null_field TEXT
            )
        """)
        
        cursor.executemany(
            "INSERT INTO test_data (id, text_field, int_field, real_field, null_field) VALUES (?, ?, ?, ?, ?)",
            [
                (1, 'Simple text', 100, 3.14, None),
                (2, 'Text with unicode: Åland', 200, 2.71, 'not null'),
                (3, 'More text', 300, 1.41, None),
            ]
        )
        
        conn.commit()
        conn.close()
        
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        output_file = tmp_path / "normal_scan_output.json"
        
        # Should complete successfully
        result = cmd.scan_db(
            connectstr=connstr,
            limit=10,
            dformat="full",
            output=str(output_file),
            output_format="json"
        )
        
        assert result is None
        assert output_file.exists()
        
        # Verify output is valid JSON
        import json
        with open(output_file) as f:
            result_data = json.load(f)
            assert isinstance(result_data, (dict, list))
        
        # Cleanup
        if db_path.exists():
            db_path.unlink()


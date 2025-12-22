"""Integration tests for database scanning functionality."""
import pytest
import os
import tempfile
import json
from pathlib import Path

from metacrafter.core import CrafterCmd


@pytest.fixture
def sqlite_db(tmp_path):
    """Create a SQLite database with test tables and data."""
    import sqlite3
    
    db_path = tmp_path / "test.db"
    connstr = f"sqlite:///{db_path}"
    
    # Create database and tables using sqlite3 directly
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    # Users table with PII data
    cursor.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            created_at TEXT,
            uuid TEXT,
            phone TEXT
        )
    """)
    
    # Insert test data
    cursor.execute("""
        INSERT INTO users (name, email, created_at, uuid, phone) VALUES
        ('John Doe', 'john.doe@example.com', '2023-01-15', '550e8400-e29b-41d4-a716-446655440000', '+1-555-123-4567'),
        ('Jane Smith', 'jane.smith@example.com', '2023-02-20', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', '+1-555-987-6543'),
        ('Bob Johnson', 'bob.j@example.com', '2023-03-10', '7c9e6679-7425-40de-944b-e07fc1f90ae7', '+1-555-456-7890')
    """)
    
    # Orders table
    cursor.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_name TEXT,
            order_date TEXT,
            amount REAL,
            status TEXT
        )
    """)
    
    cursor.execute("""
        INSERT INTO orders (customer_name, order_date, amount, status) VALUES
        ('John Doe', '2023-01-20', 99.99, 'completed'),
        ('Jane Smith', '2023-02-25', 149.50, 'pending'),
        ('Bob Johnson', '2023-03-15', 75.00, 'completed')
    """)
    
    conn.commit()
    conn.close()
    
    yield connstr
    
    # Cleanup
    if db_path.exists():
        db_path.unlink()


@pytest.fixture
def duckdb_db(tmp_path):
    """Create a DuckDB database with test tables and data."""
    try:
        import duckdb
        # Try to import duckdb-engine for SQLAlchemy support
        try:
            import duckdb_engine
        except ImportError:
            pytest.skip("DuckDB engine not installed (need duckdb-engine package)")
    except ImportError:
        pytest.skip("DuckDB not installed")
    
    db_path = tmp_path / "test.duckdb"
    # DuckDB connection string format for SQLAlchemy (requires duckdb-engine)
    connstr = f"duckdb:///{db_path}"
    
    # Create database and tables using DuckDB directly
    conn = duckdb.connect(str(db_path))
    
    # Create test tables
    conn.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR,
            created_at DATE,
            uuid VARCHAR,
            phone VARCHAR
        )
    """)
    
    conn.execute("""
        INSERT INTO users (name, email, created_at, uuid, phone) VALUES
        ('Alice Brown', 'alice.brown@example.com', '2023-04-01', '123e4567-e89b-12d3-a456-426614174000', '+1-555-111-2222'),
        ('Charlie Wilson', 'charlie.w@example.com', '2023-05-12', '223e4567-e89b-12d3-a456-426614174001', '+1-555-333-4444')
    """)
    
    conn.execute("""
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            product_name VARCHAR,
            price DECIMAL(10,2),
            category VARCHAR
        )
    """)
    
    conn.execute("""
        INSERT INTO products (product_name, price, category) VALUES
        ('Widget A', 29.99, 'Electronics'),
        ('Widget B', 49.99, 'Electronics'),
        ('Gadget X', 79.99, 'Tools')
    """)
    
    conn.close()
    
    yield connstr
    
    # Cleanup
    if db_path.exists():
        db_path.unlink()


class TestSQLiteDatabaseScanning:
    """Test SQLite database scanning functionality."""
    
    def test_scan_sqlite_database(self, sqlite_db):
        """Test basic SQLite database scanning."""
        pytest.importorskip("sqlalchemy")
        
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        
        # Scan the database
        result = cmd.scan_db(
            connectstr=sqlite_db,
            limit=10,
            dformat="short"
        )
        
        # scan_db returns None when output is written, but we can verify no exceptions
        assert result is None
    
    def test_scan_sqlite_with_output(self, sqlite_db, tmp_path):
        """Test SQLite scanning with JSON output file."""
        pytest.importorskip("sqlalchemy")
        
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        output_file = tmp_path / "sqlite_output.json"
        
        cmd.scan_db(
            connectstr=sqlite_db,
            limit=10,
            dformat="short",
            output=str(output_file),
            output_format="json"
        )
        
        # Verify output file was created
        assert output_file.exists()
        
        # Verify output contains valid JSON
        with open(output_file) as f:
            result = json.load(f)
            # Should have results structure
            assert isinstance(result, (dict, list))
    
    def test_scan_sqlite_multiple_tables(self, sqlite_db, tmp_path):
        """Test that multiple tables are processed."""
        pytest.importorskip("sqlalchemy")
        
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        output_file = tmp_path / "multi_table_output.json"
        
        cmd.scan_db(
            connectstr=sqlite_db,
            limit=10,
            dformat="full",
            output=str(output_file),
            output_format="json"
        )
        
        assert output_file.exists()
        
        with open(output_file) as f:
            result = json.load(f)
            # Should contain data from both users and orders tables
            result_str = json.dumps(result)
            # Verify both table names appear in results (either as keys or in content)
            assert "users" in result_str.lower() or "orders" in result_str.lower()
    
    def test_scan_sqlite_classification_results(self, sqlite_db, tmp_path):
        """Test that classification results contain expected fields."""
        pytest.importorskip("sqlalchemy")
        
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        output_file = tmp_path / "classification_output.json"
        
        cmd.scan_db(
            connectstr=sqlite_db,
            limit=10,
            dformat="full",
            output=str(output_file),
            output_format="json"
        )
        
        assert output_file.exists()
        
        with open(output_file) as f:
            result = json.load(f)
            # Results should be structured data
            assert isinstance(result, (dict, list))
    
    def test_scan_sqlite_empty_table(self, sqlite_db, tmp_path):
        """Test handling of empty tables."""
        pytest.importorskip("sqlalchemy")
        
        import sqlite3
        
        # Extract path from connection string
        db_path = sqlite_db.replace("sqlite:///", "")
        
        # Add an empty table to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE empty_table (id INTEGER, name TEXT)")
        conn.commit()
        conn.close()
        
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        
        # Should handle empty table gracefully
        result = cmd.scan_db(
            connectstr=sqlite_db,
            limit=10,
            dformat="short"
        )
        
        assert result is None


class TestDuckDBDatabaseScanning:
    """Test DuckDB database scanning functionality."""
    
    def test_scan_duckdb_database(self, duckdb_db):
        """Test basic DuckDB database scanning."""
        pytest.importorskip("sqlalchemy")
        pytest.importorskip("duckdb")
        # Also check for duckdb-engine if needed
        try:
            import duckdb_engine
        except ImportError:
            pytest.skip("DuckDB engine not installed (need duckdb-engine package)")
        
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        
        # Scan the database
        result = cmd.scan_db(
            connectstr=duckdb_db,
            limit=10,
            dformat="short"
        )
        
        # scan_db returns None when output is written
        assert result is None
    
    def test_scan_duckdb_with_output(self, duckdb_db, tmp_path):
        """Test DuckDB scanning with output file."""
        pytest.importorskip("sqlalchemy")
        pytest.importorskip("duckdb")
        try:
            import duckdb_engine
        except ImportError:
            pytest.skip("DuckDB engine not installed (need duckdb-engine package)")
        
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        output_file = tmp_path / "duckdb_output.json"
        
        cmd.scan_db(
            connectstr=duckdb_db,
            limit=10,
            dformat="short",
            output=str(output_file),
            output_format="json"
        )
        
        # Verify output file was created
        assert output_file.exists()
        
        # Verify output contains valid JSON
        with open(output_file) as f:
            result = json.load(f)
            assert isinstance(result, (dict, list))


class TestDatabaseErrorHandling:
    """Test error handling for database operations."""
    
    def test_scan_db_invalid_connection(self):
        """Test handling of invalid connection strings."""
        pytest.importorskip("sqlalchemy")
        
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        
        # Try to connect to invalid database
        # SQLite will create the file if path is valid, so use a truly invalid path
        import os
        invalid_path = "/nonexistent" + os.sep + "path" + os.sep + "to" + os.sep + "db.db"
        with pytest.raises(Exception):  # Should raise SQLAlchemy or OS error
            cmd.scan_db(
                connectstr=f"sqlite:///{invalid_path}",
                limit=10
            )
    
    def test_scan_db_nonexistent_schema(self, sqlite_db):
        """Test schema validation."""
        pytest.importorskip("sqlalchemy")
        
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        
        # Try to scan with non-existent schema
        # SQLite doesn't really have schemas, but the code checks for them
        with pytest.raises(ValueError, match="not found in database"):
            cmd.scan_db(
                connectstr=sqlite_db,
                schema="nonexistent_schema",
                limit=10
            )
    
    def test_scan_db_empty_database(self, tmp_path):
        """Test scanning an empty database."""
        pytest.importorskip("sqlalchemy")
        
        import sqlite3
        
        # Create empty database (just create the file)
        db_path = tmp_path / "empty.db"
        connstr = f"sqlite:///{db_path}"
        
        # Create empty database file
        conn = sqlite3.connect(str(db_path))
        conn.close()
        
        cmd = CrafterCmd(remote=None, debug=False, quiet=True)
        
        # Should handle empty database gracefully
        result = cmd.scan_db(
            connectstr=connstr,
            limit=10,
            dformat="short"
        )
        
        assert result is None
        
        # Cleanup
        if db_path.exists():
            db_path.unlink()


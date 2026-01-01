#!/usr/bin/env python3
"""
Convert DuckDB database to SQLite database.
Handles nested data structures by converting them to JSON strings.
"""
import json
import sqlite3
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("Error: duckdb package is required. Install it with: pip install duckdb")
    sys.exit(1)


def get_table_schema(conn, table_name):
    """Get column information for a table."""
    result = conn.execute(f"DESCRIBE {table_name}").fetchall()
    return result


def convert_value(value):
    """Convert DuckDB values to SQLite-compatible values."""
    if value is None:
        return None
    # Convert complex types (lists, dicts) to JSON strings
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    # Convert other types to their string representation
    return str(value)


def export_table(duckdb_conn, sqlite_conn, table_name):
    """Export a table from DuckDB to SQLite."""
    # Get schema
    schema = get_table_schema(duckdb_conn, table_name)
    
    # Build CREATE TABLE statement
    columns = []
    for col_info in schema:
        col_name = col_info[0]
        col_type = col_info[1].upper()
        
        # Map DuckDB types to SQLite types
        if 'VARCHAR' in col_type or 'TEXT' in col_type or 'CHAR' in col_type:
            sqlite_type = 'TEXT'
        elif 'INTEGER' in col_type or 'BIGINT' in col_type or 'SMALLINT' in col_type or 'TINYINT' in col_type:
            sqlite_type = 'INTEGER'
        elif 'DOUBLE' in col_type or 'FLOAT' in col_type or 'REAL' in col_type or 'DECIMAL' in col_type or 'NUMERIC' in col_type:
            sqlite_type = 'REAL'
        elif 'BOOLEAN' in col_type:
            sqlite_type = 'INTEGER'  # SQLite uses INTEGER for booleans
        elif 'DATE' in col_type or 'TIMESTAMP' in col_type or 'TIME' in col_type:
            sqlite_type = 'TEXT'
        elif 'BLOB' in col_type or 'BYTE' in col_type:
            sqlite_type = 'BLOB'
        else:
            # For complex types (STRUCT, LIST, etc.), store as TEXT (JSON)
            sqlite_type = 'TEXT'
        
        columns.append(f"{col_name} {sqlite_type}")
    
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
    sqlite_conn.execute(create_sql)
    
    # Get all data
    data = duckdb_conn.execute(f"SELECT * FROM {table_name}").fetchall()
    
    # Get column names
    column_names = [col[0] for col in schema]
    placeholders = ','.join(['?' for _ in column_names])
    insert_sql = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({placeholders})"
    
    # Insert data
    for row in data:
        converted_row = [convert_value(val) for val in row]
        sqlite_conn.execute(insert_sql, converted_row)
    
    sqlite_conn.commit()
    print(f"Exported {len(data)} rows from table '{table_name}'")


def convert_duckdb_to_sqlite(duckdb_path, sqlite_path):
    """Convert DuckDB database to SQLite."""
    # Connect to DuckDB
    duckdb_conn = duckdb.connect(str(duckdb_path))
    
    # Get list of tables
    tables = duckdb_conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    
    if not tables:
        print("No tables found in DuckDB database")
        duckdb_conn.close()
        return
    
    # Create SQLite database
    if sqlite_path.exists():
        sqlite_path.unlink()  # Remove existing file
    
    sqlite_conn = sqlite3.connect(str(sqlite_path))
    
    try:
        # Export each table
        for (table_name,) in tables:
            print(f"Exporting table: {table_name}")
            export_table(duckdb_conn, sqlite_conn, table_name)
        
        print(f"\nSuccessfully converted DuckDB database to SQLite: {sqlite_path}")
    finally:
        sqlite_conn.close()
        duckdb_conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python duckdb_to_sqlite.py <input.duckdb> <output.db>")
        sys.exit(1)
    
    duckdb_path = Path(sys.argv[1])
    sqlite_path = Path(sys.argv[2])
    
    if not duckdb_path.exists():
        print(f"Error: DuckDB file not found: {duckdb_path}")
        sys.exit(1)
    
    convert_duckdb_to_sqlite(duckdb_path, sqlite_path)


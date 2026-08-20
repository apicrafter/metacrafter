---
title: "Database scanning"
description: "Classify columns in SQL databases and MongoDB"
---
# Database scanning

## SQL (SQLAlchemy)

Any database with a SQLAlchemy dialect works. Install the driver in the same
environment (`psycopg2`, `pymysql`, `pyodbc`, …).

**PostgreSQL** — all schemas:

```bash
metacrafter scan sql "postgresql+psycopg2://username:password@127.0.0.1:15432/dbname" \
  --format short \
  --output-format json \
  --stdout
```

**PostgreSQL** — one schema, CSV summary:

```bash
metacrafter scan sql "postgresql+psycopg2://username:password@127.0.0.1:15432/dbname" \
  --schema public \
  --format full \
  --output-format csv \
  -o db_results.csv
```

**SQLite:**

```bash
metacrafter scan sql "sqlite:///path/to/database.db" \
  --format full \
  --output-format json \
  -o sqlite_results.json
```

**MySQL / MariaDB:**

```bash
metacrafter scan sql "mysql+pymysql://user:password@localhost:3306/dbname" \
  --format full \
  --output-format json \
  -o mysql_results.json
```

**DuckDB** (driver ships with Metacrafter):

```bash
metacrafter scan sql "duckdb:///path/to/database.duckdb" \
  --format full \
  --output-format json \
  -o duckdb_results.json
```

**SQL Server / Oracle:**

```bash
metacrafter scan sql "mssql+pyodbc://user:password@server/dbname?driver=ODBC+Driver+17+for+SQL+Server" \
  --format full -o sqlserver_results.json

metacrafter scan sql "oracle+cx_oracle://user:password@host:1521/service_name" \
  --format full -o oracle_results.json
```

### Batching and progress

```bash
metacrafter scan sql "postgresql://user:pass@localhost/db" \
  --batch-size 1000 \
  --progress \
  --format full \
  -o results.json
```

## MongoDB

```bash
metacrafter scan mongodb localhost \
  --port 27017 \
  --dbname mydatabase \
  --output-format json \
  -o mongodb_results.json
```

With authentication or a replica-set URI:

```bash
metacrafter scan mongodb localhost \
  --port 27017 \
  --dbname mydatabase \
  --username admin \
  --password secret \
  --format full \
  -o mongodb_results.json

metacrafter scan mongodb "mongodb://user:pass@host1:27017,host2:27017/dbname?replicaSet=rs0" \
  --format full \
  -o mongodb_results.json
```

## Related docs

- [scan sql](/commands/scan-sql)
- [scan mongodb](/commands/scan-mongodb)
- [PII detection](/use-cases/pii-detection)

"""Database source adapters; identifiers are handled by their dialect."""

from metacrafter.reports import validate_options
from metacrafter.exceptions import require_extra


def scan_items(service, items, limit, options):
    if service.remote is None:
        return service.scan_data(items, limit=limit, **options)
    return service.scan_data_client(service.remote, items, limit=limit, **options)


def scan_sql(service, connectstr, schema, limit, options, batch_size=1000, tables=None):
    from sqlalchemy import MetaData, Table, create_engine, event, inspect, select

    validate_options(limit, options.get("confidence") or 0)
    engine = create_engine(connectstr)
    reports, errors = {}, []
    if engine.dialect.name == "sqlite":

        def replace_invalid_text(dbapi_connection, _):
            dbapi_connection.text_factory = lambda value: value.decode(
                "utf8", errors="replace"
            )

        event.listen(engine, "connect", replace_invalid_text)
    try:
        with engine.connect() as connection:
            inspector = inspect(connection)
            schemas = inspector.get_schema_names()
            if schema and schema not in schemas:
                raise ValueError("Schema not found in database")
            for namespace in ([schema] if schema else schemas):
                for name in sorted(inspector.get_table_names(schema=namespace)):
                    source = "%s.%s" % (namespace, name) if namespace else name
                    if tables and name not in tables and source not in tables:
                        continue
                    try:
                        table = Table(
                            name, MetaData(), schema=namespace, autoload_with=connection
                        )
                        with connection.execute(select(table).limit(limit)) as result:
                            items = [dict(row) for row in result.mappings()]
                        report = scan_items(service, items, limit, options)
                        report.update(source=source, table=source)
                        report["sampling"].update(
                            method="database-head", population_count_known=False
                        )
                        reports[source] = report
                    except Exception as error:
                        errors.append(
                            {
                                "source": source,
                                "code": type(error).__name__,
                                "message": "Table scan failed",
                            }
                        )
    finally:
        engine.dispose()
    return reports, errors


def scan_duckdb(service, connectstr, schema, limit, options):
    duckdb = require_extra("duckdb", "duckdb", "Native DuckDB scanning")

    validate_options(limit, options.get("confidence") or 0)
    path = connectstr.split("///", 1)[-1]
    quote = lambda name: '"' + name.replace('"', '""') + '"'
    reports, errors = {}, []
    with duckdb.connect(path, read_only=True) as connection:
        tables = connection.execute(
            "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type='BASE TABLE' ORDER BY table_schema, table_name"
        ).fetchall()
        if schema and schema not in [row[0] for row in tables]:
            raise ValueError("Schema not found in database")
        for namespace, name in tables:
            if schema and namespace != schema:
                continue
            source = namespace + "." + name
            try:
                result = connection.execute(
                    "SELECT * FROM %s.%s LIMIT ?" % (quote(namespace), quote(name)),
                    [limit],
                )
                names = [col[0] for col in result.description]
                items = [dict(zip(names, row)) for row in result.fetchall()]
                report = scan_items(service, items, limit, options)
                report.update(source=source, table=source)
                report["sampling"].update(
                    method="database-head", population_count_known=False
                )
                reports[source] = report
            except Exception as error:
                errors.append(
                    {
                        "source": source,
                        "code": type(error).__name__,
                        "message": "Table scan failed",
                    }
                )
    return reports, errors


def scan_mongo(
    service, host, port, dbname, username, password, limit, options, batch_size=1000
):
    MongoClient = require_extra("pymongo", "mongodb", "MongoDB scanning").MongoClient

    validate_options(limit, options.get("confidence") or 0)
    reports, errors = {}, []
    with MongoClient(host, port, username=username, password=password) as client:
        database = client[dbname]
        for name in sorted(database.list_collection_names()):
            source = dbname + "." + name
            try:
                with database[name].find().batch_size(batch_size).limit(
                    limit
                ) as cursor:
                    items = list(cursor)
                report = scan_items(service, items, limit, options)
                report.update(source=source, table=source)
                report["sampling"].update(
                    method="database-head", population_count_known=False
                )
                reports[source] = report
            except Exception as error:
                errors.append(
                    {
                        "source": source,
                        "code": type(error).__name__,
                        "message": "Collection scan failed",
                    }
                )
    return reports, errors

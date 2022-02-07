#!/usr/bin/env python
# -*- coding: utf8 -*-
import os
import logging
import csv
import qddate.patterns
import qddate
import click
import orjson
from tabulate import tabulate
from metacrafter.classify.processor import RulesProcessor
from metacrafter.classify.stats import Analyzer
from metacrafter.classify.utils import detect_delimiter, detect_encoding
import yaml
import bson
import pandas
import json

SUPPORTED_FILE_TYPES = ['jsonl', 'bson', 'csv', 'json']
BINARY_DATA_FORMATS = ['bson', 'parquet']

DEFAULT_METACRAFTER_CONFIGFILE = '.metacrafter'
DEFAULT_RULEPATH = ['rules',]

class CrafterCmd(object):

    def __init__(self):
        self.processor = RulesProcessor()
        self.prepare()
        pass


    def run_classifier_server(self):
        logging.info('Run server with classifier API')
        from metacrafter.server.manager import run_server
        self.prepare()
        run_server()


    def prepare(self):
        rulepath = []
        if os.path.exists(DEFAULT_METACRAFTER_CONFIGFILE):
            f = open(DEFAULT_METACRAFTER_CONFIGFILE, 'r', encoding='utf8')
            config = yaml.load(f, Loader=yaml.FullLoader)
            f.close()
            if config:
                if 'rulepath' in config.keys():
                    rulepath = config['rulepath']
        else:
            rulepath = DEFAULT_RULEPATH
        for rp in rulepath:
            self.processor.import_rules_path(rp, recursive=True)
        self.dparser = qddate.DateParser(patterns=qddate.patterns.PATTERNS_EN + qddate.patterns.PATTERNS_RU)

    def rules_load(self):
        self.processor.dumpStats()

    def _write_results(self, prepared, results, filename, dformat, output):
        if output:
            if output:
                f = open(output, 'w', encoding='utf8')
                out = []
                f.write(json.dumps({'table': filename, 'fields' : results}, indent=4, sort_keys=True))
                f.close()
                print('Output written to %s' % (output))
        elif len(prepared) > 0:
            outres = []
            if dformat == 'short':
                for r in prepared:
                    if len(r[3]) > 0:
                        outres.append(r)
                headers = ['key', 'ftype', 'tags', 'matches']
            elif dformat == 'full':
                outres = prepared
                headers = ['key', 'ftype', 'tags', 'matches']
            if len(outres) > 0:
                print(tabulate(outres, headers=headers))

    def _write_db_results(self, db_results, dformat, output):
        out = []
        for table, data in db_results.items():
            prepared, results = data
            if output:
                out.append({'table': table, 'fields': results})
            else:
                outres = []
                if dformat == 'short':
                    for r in prepared:
                        if len(r[3]) > 0:
                            outres.append(r)
                    headers = ['key', 'ftype', 'tags', 'matches']
                elif dformat == 'full':
                    outres = prepared
                    headers = ['key', 'ftype', 'tags', 'matches']
                if len(outres) > 0:
                    print(tabulate(outres, headers=headers))
        if output:
            print('Output written to %s' % (output))
            f = open(output, 'w', encoding='utf8')
            f.write(json.dumps(out, indent=4, sort_keys=True))
            f.close()

    def scan_data(self, items, limit=1000, contexts=None, langs=None):
        # load rules since file is acceptable

        analyzer = Analyzer()
        datastats = analyzer.analyze(fromfile=None, itemlist=items, options={'delimiter' : ',', 'format_in': None, 'zipfile': None})
        headers = ['key', 'ftype', 'is_dictkey', 'is_uniq', 'n_uniq', 'share_uniq', 'minlen', 'maxlen', 'avglen', 'tags']
        datastats_dict = {}
        for row in datastats:
            datastats_dict[row[0]] = {}
            for n in range(0, len(headers)):
                datastats_dict[row[0]][headers[n]] = row[n]

        results = self.processor.match_dict(items, datastats=datastats_dict, confidence=5, dateparser=self.dparser, parse_dates=True, limit=limit, filter_contexts=contexts, filter_langs=langs)
        output = []
        outdata = []
        for res in results.results:
            matches = []
            for match in res.matches:
                s = "%s %0.2f" % (match.dataclass, match.confidence)
                if match.format is not None:
                    s += ' (%s)' % (match.format)
                matches.append(s)
            if res.field not in datastats_dict.keys():
                continue
            output.append([res.field, datastats_dict[res.field]['ftype'], ','.join(datastats_dict[res.field]['tags']), ','.join(matches)])
            record = res.asdict()
            record['tags'] = datastats_dict[res.field]['tags']
            record['ftype'] = datastats_dict[res.field]['ftype']
            outdata.append(record)
        return output, outdata

    def scan_file(self, filename, delimiter=',', limit=1000, encoding='utf8', contexts=None, langs=None, dformat='short', output=None):
        ext = filename.rsplit('.', 1)[-1]
        if ext not in BINARY_DATA_FORMATS:
            encoding_dec = detect_encoding(filename)
            if encoding_dec:
                encoding = encoding_dec['encoding']
        filetype = None
        if ext == 'csv':
            filetype = 'csv'
            detected_delimiter = detect_delimiter(filename, encoding)
            if detected_delimiter:
                delimiter = detected_delimiter
            items = []
            f = open(filename, 'r', encoding=encoding)
            reader = csv.DictReader(f, delimiter=delimiter)
            n = 0
            for row in reader:
                n += 1
                if n > limit:
                    break
                items.append(row)
            f.close()
        elif ext == 'jsonl':
            filetype = 'jsonl'
            f = open(filename, 'r', encoding=encoding)
            items = []
            n = 0
            for l in f:
                n += 1
                if n > limit:
                    break
                items.append(orjson.loads(l))
            f.close()
        elif ext == 'bson':
            filetype = 'bson'
            f = open(filename, 'rb')
            items = []
            n = 0
            for l in bson.decode_file_iter(f):
                n += 1
                if n > limit:
                    break
                items.append(l)
            f.close()
        elif ext == 'json':
            filetype = 'json'
            f = open(filename, 'r', encoding=encoding)
            items = []
            n = 0
            data = json.load(f)
            if not isinstance(data, list) and not isinstance(data[0], dict):
                print(
                    'Unsupported JSON file. It should be "array JSON" with list of objects. Please preprocess data to this format')
                return []
            for l in data:
                n += 1
                if n > limit:
                    break
                items.append(l)
            f.close()
        elif ext == 'parquet':
            filetype = 'parquet'
            tbl = pandas.read_parquet(filename)
            items = []
            n = 0
            ad = tbl.to_dict('records')
            for l in ad:
                n += 1
                if n > limit:
                    break
                items.append(l)

        else:
            print('Unsupported file type. Supported file types are CSV, JSON lines, BSON, Parquet, JSON. Empty results')
            return []

        print('Filetype idenfied as %s' % (filetype))
        print('Processing file %s' % (filename))

        prepared, results  = self.scan_data(items, limit, contexts, langs)
        self._write_results(prepared, results, filename, dformat, output)


    def scan_db(self, connectstr='sqlite:///test.db', schema=None, limit=1000, contexts=None, langs=None, dformat='short', output=None):
        """SQL alchemy way to scan any database"""
        from sqlalchemy import create_engine, inspect
        import sqlalchemy.exc
        dbtype = connectstr.split(':', 1)[0].lower()
        print('Connecting to %s' % (connectstr))
        dbe = create_engine(connectstr)
        inspector = inspect(dbe)
        db_schemas = inspector.get_schema_names()
        con = dbe.connect()
        db_results = {}
        for db_schema in db_schemas:
            if schema and schema != db_schema:
                continue
            print("Processing schema: %s" % schema)
            if dbtype == 'postgres':
                con.execute('SET search_path TO {schema}'.format(schema=schema))
            for table in inspector.get_table_names(schema=schema):
                print('- table %s' % (table))
                try:
                    query = 'SELECT * FROM %s LIMIT %d' % (table, limit)
                    queryres = con.execute(query)
                except sqlalchemy.exc.ProgrammingError as e:
                    print('Error processing table %s: %s' % (table, str(e)))
                items = [dict(u) for u in queryres.fetchall()]
                prepared, results = self.scan_data(items, limit, contexts, langs)
                db_results[table] = [prepared, results]
        self._write_db_results(db_results, dformat, output)

    def scan_mongodb(self, host='localhost', port=27017, dbname='test', username=None, password=None, limit=1000, contexts=None, langs=None, dformat='short', output=None):
        """Scan entire MongoDB database"""
        print('Connecting to %s %d' % (host, port))
        from pymongo import MongoClient
        client = MongoClient(host, port, username=username, password=password)
        db = client[dbname]
        tables = db.list_collection_names()
        db_results = {}
        for table in tables:
            print('- table %s' % (table))
            items = list(db[table].find().limit(limit))
            prepared, results = self.scan_data(items, limit, contexts, langs)
            db_results[table] = [prepared, results]
        self._write_db_results(db_results, dformat, output)


@click.group()
def cli1():
    pass


@cli1.command()
def server():
    """Starts API and web interface for data management"""
    acmd = CrafterCmd()
    acmd.run_classifier_server()


@click.group()
def cli2():
    pass

@cli2.command()
def rules():
    """Rules load and test"""
    acmd = CrafterCmd()
    acmd.rules_load()

@click.group()
def cli3():
    pass

@cli3.command()
@click.argument('filename')
@click.option('--delimiter', '-d', default=',', help='CSV delimiter')
@click.option('--limit', '-n', default='1000', help='Limit of records')
@click.option('--contexts', '-x', default=None, help='List of contexts to use. Comma separates')
@click.option('--langs', '-l', default=None, help='List of languages to use. Comma separated')
@click.option('--format', '-f', default='short', help='Output format: short, long')
@click.option('--output', '-o', default=None, help='Output JSON filename')
def scan_file(filename, delimiter, limit, contexts, langs, format, output):
    """Match file"""
    acmd = CrafterCmd()
    acmd.scan_file(filename, delimiter, int(limit), contexts, langs, dformat=format, output=output)

@click.group()
def cli4():
    pass

@cli4.command()
@click.option('--connstr', '-c', default=None, help='SQLAlchemy connection string')
@click.option('--schema', '-s', default=None, help='Database schema. For Postgres DBs')
@click.option('--limit', '-n', default='1000', help='Limit of records')
@click.option('--contexts', '-x', default=None, help='List of contexts to use. Comma separates')
@click.option('--langs', '-l', default=None, help='List of languages to use. Comma separated')
@click.option('--format', '-f', default='short', help='Output format: short, long')
@click.option('--output', '-o', default=None, help='Output JSON filename')
def scan_db(connstr, schema, limit, contexts, langs, format, output):
    """Scan database using SQL alchemy connection string"""
    acmd = CrafterCmd()
    acmd.scan_db(connstr, schema, limit=int(limit), contexts=contexts, langs=langs, dformat=format, output=output)

@click.group()
def cli5():
    pass

@cli5.command()
@click.option('--host', '-h', default='localhost', help='SQLAlchemy connection string')
@click.option('--port', '-p', default=27017, help='Database schema. For Postgres DBs')
@click.option('--dbname', '-d', default="test", help='Database name')
@click.option('--username', '-u', default=None, help='Username. Optional')
@click.option('--password', '-P', default=None, help='Password. Optional')
@click.option('--limit', '-n', default='1000', help='Limit of records')
@click.option('--contexts', '-x', default=None, help='List of contexts to use. Comma separates')
@click.option('--langs', '-l', default=None, help='List of languages to use. Comma separated')
@click.option('--format', '-f', default='short', help='Output format: short, long')
@click.option('--output', '-o', default=None, help='Output JSON filename')
def scan_mongodb(host, port, dbname, username, password, limit, contexts, langs, format, output):
    """Scan MongoDB database"""
    acmd = CrafterCmd()
    acmd.scan_mongodb(host, int(port), dbname, username, password, limit=int(limit), contexts=contexts, langs=langs, dformat=format, output=output)



cli = click.CommandCollection(sources=[cli1, cli2, cli3, cli4, cli5])

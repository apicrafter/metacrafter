#!/usr/bin/env python
# -*- coding: utf8 -*-
import json
import logging
import os


import typer

import qddate
import qddate.patterns
import yaml
from tabulate import tabulate
import csv
import requests

from iterable.helpers.detect import open_iterable

from metacrafter.classify.processor import RulesProcessor, BASE_URL
from metacrafter.classify.stats import Analyzer



SUPPORTED_FILE_TYPES = ["jsonl", "bson", "csv", "tsv", "json", "xml", 'ndjson', 'avro', 'parquet', 'xls', 'xlsx', 'orc', 'ndjson']
CODECS = ["lz4", 'gz', 'xz', 'bz2', 'zst', 'br', 'snappy']
BINARY_DATA_FORMATS = ["bson", "parquet"]

DEFAULT_METACRAFTER_CONFIGFILE = ".metacrafter"
DEFAULT_RULEPATH = [
    "rules",
]


app = typer.Typer()
rules_app = typer.Typer()
app.add_typer(rules_app, name='rules')

scan_app = typer.Typer()
app.add_typer(scan_app, name='scan')

server_app = typer.Typer()
app.add_typer(server_app, name='server')


class CrafterCmd(object):
    def __init__(self, remote:str=None, debug:bool=False):
        # logging.getLogger().addHandler(logging.StreamHandler())
        if debug:
            logging.basicConfig(
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                level=logging.DEBUG,
            )
        self.remote = remote
        if remote is None:
            self.processor = RulesProcessor()
            self.prepare()
      
        pass

    def prepare(self):
        rulepath = []
        filepath = None
        if os.path.exists(DEFAULT_METACRAFTER_CONFIGFILE):
            logging.debug("Local .metacrafter config exists. Using it")
            filepath = DEFAULT_METACRAFTER_CONFIGFILE
        elif os.path.exists(
            os.path.join(os.path.expanduser("~"), DEFAULT_METACRAFTER_CONFIGFILE)
        ):
            logging.debug("Home dir .metacrafter config exists. Using it")
            filepath = os.path.join(
                os.path.expanduser("~"), DEFAULT_METACRAFTER_CONFIGFILE
            )
        if filepath:
            f = open(filepath, "r", encoding="utf8")
            config = yaml.load(f, Loader=yaml.FullLoader)
            f.close()
            if config:
                if "rulepath" in config.keys():
                    rulepath = config["rulepath"]
        else:
            rulepath = DEFAULT_RULEPATH
        for rp in rulepath:
            self.processor.import_rules_path(rp, recursive=True)
        self.dparser = qddate.DateParser(
            patterns=qddate.patterns.PATTERNS_EN + qddate.patterns.PATTERNS_RU
        )

    def rules_list(self):
        """Rules list"""
        headers = ['id', 'name', 'type', 'match', 'group', 'group_desc', 'lang']
        all_rules = []
        for item in self.processor.field_rules:
            rule = []
            for h in headers:
                rule.append(item[h])
            rule.append(','.join(item['context']))                             
            all_rules.append(rule)

        for item in self.processor.data_rules:
            rule = []
            for h in headers:
                rule.append(item[h])
            rule.append(','.join(item['context']))        
            all_rules.append(rule)

        for pat in self.dparser.patterns:
            rule = [pat['key'], pat['name'], 'data', 'ppr', 'datetime', "qddate datetime patterns", 'common', 'datetime']    
            all_rules.append(rule) 
        headers.append('context')            
#        print(all_rules)
        print(tabulate(all_rules, headers=headers))            

    def rules_dumpstats(self):
        """Dump rules statistics"""

        print("Rule types:")
        print("- field based rules %d" % (len(self.processor.field_rules)))
        print("- data based rules %d" % (len(self.processor.data_rules)))
        print("Context:")
        for key in sorted(self.processor.contexts.keys()):
            print("- %s %d" % (key, self.processor.contexts[key]))
        print("Language:")
        for key in sorted(self.processor.langs.keys()):
            print("- %s %d" % (key, self.processor.langs[key]))
        print("Data/time patterns (qddate): %d" % (len(self.dparser.patterns)))

    def _write_results(self, prepared, results, filename, dformat, output):
        if output:
            if isinstance(output, str):
                f = open(output, "w", encoding="utf8")
                out = []
                if output.rsplit('.', 1)[-1].lower() == 'csv':
                    if dformat == "short":
                        for r in prepared:
                            if len(r[3]) > 0:
                                outres.append(r)
                        headers = ["key", "ftype", "tags", "matches", "datatype_url"]
                    elif dformat in ["full", "long"]:
                        outres = prepared
                        headers = ["key", "ftype", "tags", "matches", "datatype_url"]
                    writer = csv.writer(f)
                    writer.writerow(headers)
                    writer.writerows(outres)
                else:
                    f.write(
                        json.dumps(
                            {"table": filename, "fields": results},
                            indent=4,
                            sort_keys=True,
                            ensure_ascii=False,
                        )
                    )
                f.close()
                print("Output written to %s" % (output))
            else:
                    output.write(
                        json.dumps(
                            {"table": filename, "fields": results},
                            ensure_ascii=False,
                        ) + '\n'
                    )
        elif len(prepared) > 0:
            outres = []
            if dformat == "short":
                for r in prepared:
                    if len(r[3]) > 0:
                        outres.append(r)
                headers = ["key", "ftype", "tags", "matches", "datatype_url"]
            elif dformat in ["full", "long"]:
                outres = prepared
                headers = ["key", "ftype", "tags", "matches", "datatype_url"]
            else:
                print("Unknown output format %s" % (dformat))
            if len(outres) > 0:
                print(tabulate(outres, headers=headers))
            else:
                print("No results")
        else:
            print("No results")

    def _write_db_results(self, db_results, dformat, output):
        out = []
        for table, data in db_results.items():
            prepared, results = data
            if output:
                out.append({"table": table, "fields": results})
            else:
                outres = []
                if dformat == "short":
                    for r in prepared:
                        if len(r[3]) > 0:
                            outres.append(r)
                    headers = ["key", "ftype", "tags", "matches", "datatype_url"]
                elif dformat == "full":
                    outres = prepared
                    headers = ["key", "ftype", "tags", "matches", "datatype_url"]
                if len(outres) > 0:
                    print("Table: %s" % (table))
                    print(tabulate(outres, headers=headers))
                    print()
        if output:
            print("Output written to %s" % (output))
            f = open(output, "w", encoding="utf8")
            f.write(json.dumps(out, indent=4, sort_keys=True))
            f.close()


    def scan_data_client(self, api_root, items, limit=1000, contexts=None, langs=None):
        params = {'langs' : ','.join(langs) if langs else None, 'contexts' : ','.join(contexts) if contexts else None}

        url = api_root + '/api/v1/scan_data'

        headers = {
        'Content-Type': 'application/json'
        }
        payload = json.dumps(items)
                
        report = requests.request("POST", url, headers=headers, data=payload, params=params).json()
        return report

    def scan_data(self, items, limit=1000, contexts=None, langs=None):
        # load rules since file is acceptable

        analyzer = Analyzer()
        datastats = analyzer.analyze(
            fromfile=None,
            itemlist=items,
            options={"delimiter": ",", "format_in": None, "zipfile": None},
        )
        headers = [
            "key",
            "ftype",
            "is_dictkey",
            "is_uniq",
            "n_uniq",
            "share_uniq",
            "minlen",
            "maxlen",
            "avglen",
            "tags",
            "has_digit",
            "has_alphas",
            "has_special",
            "dictvalues",
        ]
        datastats_dict = {}
        for row in datastats:
            #            print(row)
            datastats_dict[row[0]] = {}
            for n in range(0, len(headers)):
                datastats_dict[row[0]][headers[n]] = row[n]

        results = self.processor.match_dict(
            items,
            datastats=datastats_dict,
            confidence=5,
            dateparser=self.dparser,
            parse_dates=True,
            limit=limit,
            filter_contexts=contexts,
            filter_langs=langs,
        )
        output = []
        outdata = []
        for res in results.results:
            matches = []
            for match in res.matches:
                s = "%s %0.2f" % (match.dataclass, match.confidence)
                if match.format is not None:
                    s += " (%s)" % (match.format)
                matches.append(s)
            if res.field not in datastats_dict.keys():
                continue
            output.append(
                [
                    res.field,
                    datastats_dict[res.field]["ftype"],
                    ",".join(datastats_dict[res.field]["tags"]),
                    ",".join(matches),
                    BASE_URL.format(dataclass=res.matches[0].dataclass)
                    if len(res.matches) > 0
                    else "",
                ]
            )
            record = res.asdict()
            record["tags"] = datastats_dict[res.field]["tags"]
            record["ftype"] = datastats_dict[res.field]["ftype"]
            record["datatype_url"] = (
                BASE_URL.format(dataclass=res.matches[0].dataclass)
                if len(res.matches) > 0
                else ""
            )
            record["stats"] = datastats_dict[res.field]

            outdata.append(record)
        report = {'results' : output, 'data' : outdata}            
        return report


    def scan_file(
        self,
        filename,
        delimiter=None,
        tagname=None,
        limit=1000,
        encoding=None,
        contexts=None,
        langs=None,
        dformat="short",
        output=None,
    ):
        iterableargs = {}
        if tagname is not None:
            iterableargs['tagname'] = tagname
        if delimiter is not None:
            iterableargs['delimiter'] = delimiter            

        if encoding is not None:
            iterableargs['encoding'] = encoding                         
                   
        try:
            data_file = open_iterable(filename, iterableargs=iterableargs) 
        except Exception as e:
            print('Exception', e)                 
            print(
                "Unsupported file type. Supported file types are CSV, TSV, JSON lines, BSON, Parquet, JSON. Empty results"
            )
            return []
        items = list(data_file)            
        if len(items) == 0:
            print("No records found to process")
            return
        print("Processing file %s" % (filename))
        print("Filetype identified as %s" % (data_file.id()))
        if self.remote is None:
            report = self.scan_data(items, limit, contexts, langs)      
        else:
            report = self.scan_data_client(self.remote, items, limit, contexts, langs)      
        self._write_results(report['results'], report['data'], filename, dformat, output)                  
        data_file.close()
        del items


    def scan_bulk(
        self,
        dirname,
        delimiter=None,
        tagname=None,
        limit=1000,
        encoding="utf8",
        contexts=None,
        langs=None,
        output=None,
    ):
        fobj = open(output, 'w', encoding='utf8')
        filelist = [os.path.join(dp, f) for dp, dn, filenames in os.walk(dirname) for f in filenames]
        
        for filename in filelist:                
            try:
                self.scan_file(filename, delimiter=delimiter, tagname=tagname, limit=limit, encoding=encoding,contexts=contexts, langs=langs, dformat='full', output=fobj)
            except Exception as e:
                print(f'Error occured {e} on {filename}')

    def scan_db(
        self,
        connectstr="sqlite:///test.db",
        schema=None,
        limit=1000,
        contexts=None,
        langs=None,
        dformat="short",
        output=None,
    ):
        """SQL alchemy way to scan any database"""
        from sqlalchemy import create_engine, inspect
        import sqlalchemy.exc

        dbtype = connectstr.split(":", 1)[0].lower()
        print("Connecting to %s" % (connectstr))
        dbe = create_engine(connectstr)
        inspector = inspect(dbe)
        db_schemas = inspector.get_schema_names()
        con = dbe.connect()
        db_results = {}
        for db_schema in db_schemas:
            if schema and schema != db_schema:
                continue
            print("Processing schema: %s" % schema)
            if dbtype == "postgres":
                con.execute("SET search_path TO {schema}".format(schema=schema))
            for table in inspector.get_table_names(schema=schema):
                print("- table %s" % (table))
                try:
                    query = "SELECT * FROM '%s' LIMIT %d" % (table, limit)
                    queryres = con.execute(query)
                except sqlalchemy.exc.ProgrammingError as e:
                    print("Error processing table %s: %s" % (table, str(e)))
                    continue
                items = [dict(u) for u in queryres.fetchall()]
                if self.remote is None:
                    report = self.scan_data(items, limit, contexts, langs)      
                else:
                    report = self.scan_data_client(self.remote, items, limit, contexts, langs)                      
                db_results[table] = [report['results'], report['data']]
        self._write_db_results(db_results, dformat, output)



    def scan_mongodb(
        self,
        host="localhost",
        port=27017,
        dbname="test",
        username=None,
        password=None,
        limit=1000,
        contexts=None,
        langs=None,
        dformat="short",
        output=None,
    ):
        """Scan entire MongoDB database"""
        print("Connecting to %s %d" % (host, port))
        from pymongo import MongoClient

        client = MongoClient(host, port, username=username, password=password)
        db = client[dbname]
        tables = db.list_collection_names()
        db_results = {}
        for table in tables:
            print("- table %s" % (table))
            items = list(db[table].find().limit(limit))
            #            print(items)
            if self.remote is None:
                report = self.scan_data(items, limit, contexts, langs)      
            else:
                report = self.scan_data_client(self.remote, items, limit, contexts, langs)
            db_results[table] = [report['results'], report['data']]
        self._write_db_results(db_results, dformat, output)


@server_app.command('run')
def server_run(host='127.0.0.1', port=10399, debug:bool=False):
    """Starts API and web interface for data management"""
    logging.info("Run server with classifier API")
    from metacrafter.server.manager import run_server

    run_server(host, port, debug)


@rules_app.command('stats')
def rules_stats(debug:bool=False):
    """Generates rules statistics """
    acmd = CrafterCmd(debug=debug)
    acmd.rules_dumpstats()

@rules_app.command('list')
def rules_list(debug:bool=False):
    """List rules"""
    acmd = CrafterCmd(debug=debug)
    acmd.rules_list()

@scan_app.command('file')
def scan_file(filename:str, delimiter:str=None, tagname:str=None, limit:int=100, contexts:str=None, langs:str=None, format:str="short", output:str=None, remote:str=None, debug:bool = False):
    """Match file"""
    acmd = CrafterCmd(remote, debug)
    acmd.scan_file(
        filename,
        delimiter,
        tagname,
        int(limit),
        contexts,
        langs,
        dformat=format,
        output=output,
    )

@scan_app.command('sql')
def scan_db(connstr:str, schema:str=None, limit:int=1000, contexts:str=None, langs:str=None, format:str="short", output:str=None, remote:str=None, debug:bool=False):
    """Scan database using SQL alchemy connection string"""
    acmd = CrafterCmd(remote, debug)
    acmd.scan_db(
        connstr,
        schema,
        limit=int(limit),
        contexts=contexts,
        langs=langs,
        dformat=format,
        output=output,
    )


@scan_app.command('mongodb')
def scan_mongodb(host:str, port:int=27017, dbname:str=None, username:str=None, password:str=None, limit:int=1000, contexts:str=None, langs:str=None, format:str="short", output:str=None, remote:str=None, debug:bool=False):
    """Scan MongoDB database"""
    acmd = CrafterCmd(remote, debug)
    acmd.scan_mongodb(
        host,
        int(port),
        dbname,
        username,
        password,
        limit=int(limit),
        contexts=contexts,
        langs=langs,
        dformat=format,
        output=output,
    )


@scan_app.command('bulk')
def scan_bulk(dirname:str, delimiter:str=None, tagname:str=None, limit:int=100, contexts:str=None, langs:str=None, format:str=None, output:str=None, remote:str=None, debug:bool=False):
    """Match group of files in a directory"""
    acmd = CrafterCmd()
    acmd.scan_bulk(
        dirname,
        delimiter,
        tagname,
        int(limit),
        contexts,
        langs,
        output=output,
    )


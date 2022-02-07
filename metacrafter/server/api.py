import io
from flask import Flask, json, jsonify, redirect, render_template, send_file, send_from_directory, request, url_for, flash, Response
import logging
import yaml
import os
import csv
import csv
import qddate

from ..classify.processor import RulesProcessor
from metacrafter.classify.stats import Analyzer

DEFAULT_METACRAFTER_CONFIGFILE  ='.metacrafter'
DEFAULT_RULEPATH = ['rules',]
MANAGE_PREFIX = ''
DEFAULT_LIMIT = 1000

def prepare(processor):
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
        processor.import_rules_path(rp, recursive=True)

def scan_data():
    output = {}
    format = request.args.get('format', default='short', type=str)
    skipempty = request.args.get('skipempty', default=0, type=int)
    langs = request.args.get('langs', default=None, type=str)
    contexts = request.args.get('contexts', default=None, type=str)
    langs = langs.split('.') if langs is not None else None
    contexts = contexts.split('.') if contexts is not None else None
    try:
        processor = RulesProcessor()
        prepare(processor)
        items = json.loads(request.data)
        analyzer = Analyzer()
        datastats = analyzer.analyze(fromfile=None, itemlist=items,
                                     options={'delimiter': ',', 'format_in': None, 'zipfile': None})
        headers = ['key', 'ftype', 'is_dictkey', 'is_uniq', 'n_uniq', 'share_uniq', 'minlen', 'maxlen', 'avglen']
        datastats_dict = {}
        for row in datastats:
            datastats_dict[row[0]] = {}
            for n in range(0, len(headers)):
                datastats_dict[row[0]][headers[n]] = row[n]

        dparser = qddate.DateParser(patterns=qddate.patterns.PATTERNS_EN + qddate.patterns.PATTERNS_RU)
        results = processor.match_dict(items, datastats=datastats_dict, confidence=5, dateparser=dparser,
                                            parse_dates=True, filter_langs=langs, filter_contexts=contexts, limit=DEFAULT_LIMIT)

        if skipempty:
            output = []
            for o in results.results:
                if len(o.matches) != 0:
                    output.append(o.asdict())
        else:
            output = results.asdict()

    except Exception as ex:
        output = {'error' : 'Exception occured', 'message' : str(ex)}
    return jsonify(output)


def add_api_rules(app, prefix=MANAGE_PREFIX):
    """Adds API related rules to the"""
    app.add_url_rule(prefix + '/api/v1/scan_data', 'classify', scan_data, methods=['POST'],)

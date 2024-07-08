import os
import logging
import qddate
import yaml
from flask import (
    json,
    jsonify,
    request,
)

from metacrafter.classify.stats import Analyzer
from ..classify.processor import RulesProcessor, BASE_URL

RULES_PROCESSOR = None
DATE_PARSER = None

DEFAULT_METACRAFTER_CONFIGFILE = ".metacrafter"
DEFAULT_RULEPATH = [
    "rules",
]
MANAGE_PREFIX = ""
DEFAULT_LIMIT = 1000


def initialize_rules():
    global RULES_PROCESSOR
    global DATE_PARSER    
    RULES_PROCESSOR = RulesProcessor()
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
        RULES_PROCESSOR.import_rules_path(rp, recursive=True)

    DATE_PARSER = qddate.DateParser(
        patterns=qddate.patterns.PATTERNS_EN + qddate.patterns.PATTERNS_RU
    )


def scan_data():
    global RULES_PROCESSOR
    global DATE_PARSER
    output = {}
    format = request.args.get("format", default="short", type=str)
    langs = request.args.get("langs", default=None, type=str)
    contexts = request.args.get("contexts", default=None, type=str)
    scan_limit = request.args.get("limit", default=1000, type=int) 
    langs = langs.split(".") if langs is not None else None
    contexts = contexts.split(".") if contexts is not None else None
    try: 
        items = json.loads(request.data)
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

        results = RULES_PROCESSOR.match_dict(
            items,
            datastats=datastats_dict,
            confidence=5,
            dateparser=DATE_PARSER,
            parse_dates=True,
            limit=scan_limit,
            filter_contexts=contexts,
            filter_langs=langs,
        )


        output = []
        outdata = []
        report = {}
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
    except KeyboardInterrupt as ex:
        report = {"error": "Exception occured", "message": str(ex)}
        logging.info(report)
    return jsonify(report)


def add_api_rules(app, prefix=MANAGE_PREFIX):
    """Adds API related rules to the"""
    app.add_url_rule(
        prefix + "/api/v1/scan_data",
        "classify",
        scan_data,
        methods=["POST"],
    )

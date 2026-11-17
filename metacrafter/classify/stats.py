# -*- coding: utf-8 -*-
"""Statistics module for analyzing data files and generating field statistics."""
import csv
import logging
import zipfile
from datetime import datetime, date
from typing import List, Dict, Any, Optional, Union, Generator, Tuple

import orjson
from qddate import DateParser
from metacrafter.exceptions import require_extra

DEFAULT_DICT_SHARE = 10
# Note: This list is for backward compatibility with Analyzer class.
# The main format support is handled by iterabledata in core.py
SUPPORTED_FILE_TYPES = [
    "csv",
    "tsv",
    "json",
    "jsonl",
    "ndjson",
    "bson",
    "parquet",
    "avro",
    "orc",
    "xls",
    "xlsx",
    "xml",
    "pickle",
    "pkl",
]

# NA is the missing value of R and of the data exported from it (TERN, Dryad, CRAN packages).
# s/d (sin datos) and sin información mark a missing value in Spanish data: the directories of
# the Argentine culture ministry fill whole columns with them (datos.cultura.gob.ar).
SPANISH_EMPTY_VALUES = [
    "s/d", "S/D", "sin dato", "Sin dato", "sin datos", "Sin datos", "Sin Datos",
    "sin_informacion", "sin información", "Sin información",
    # Colombian registers (www.datos.gov.co): No aplica in the columns of a form that do not apply to a
    # record, No registra and No reporta where a source system had no value (49 983 cells of one vital
    # statistics table), SIN DATO and SIN INFORMACIÓN in capitals and without the accent.
    "No aplica", "No Aplica", "no aplica", "NO APLICA",
    "No registra", "No Registra", "no registra", "NO REGISTRA",
    "No reporta", "No Reporta", "no reporta", "NO REPORTA",
    "SIN DATO", "Sin Dato", "SIN DATOS",
    "SIN INFORMACIÓN", "Sin Información", "SIN INFORMACION", "Sin Informacion", "Sin informacion",
    "sin informacion", "SIN INFO",
]
# NULL and null are the missing value of SQL exports, <Null> that of ArcGIS attribute tables.
NULL_EMPTY_VALUES = ["NULL", "null", "<Null>", "<NULL>"]
# Not Applicable, Not Assessed and Not Recorded stand for a missing value in English asset and
# tree registers: the City of Ballarat (Australia) writes Not Assessed in 41 712 of the 81 467 DBH
# cells of its tree register and Not Applicable in 3 405 of the 3 757 rank cells of the Avenue of
# Honour plaques.
ENGLISH_EMPTY_VALUES = [
    "n/a", "Not Applicable", "Not applicable", "not applicable", "NOT APPLICABLE",
    "Not Assessed", "Not assessed", "not assessed", "NOT ASSESSED",
    "Not Recorded", "Not recorded", "not recorded", "NOT RECORDED",
    "Not Stated", "Not stated", "not stated", "NOT STATED",
]
# غير مبين (not stated), غير محدد (not specified), غير معروف (unknown) and غير متوفر (not available),
# also in the feminine, mark a missing value in Arabic statistics: the census and vital statistics
# of the Bahrain Open Data Portal (data.gov.bh) add a row of them to tables by age group, governorate
# and nationality (52 of the 520 age-group cells of the marriage cases by age group), and they
# counted as values that do not fit a rule.
ARABIC_EMPTY_VALUES = [
    "غير مبين", "غير مبينة", "غير محدد", "غير محددة", "غير معروف", "غير معروفة", "غير متوفر", "غير متوفرة",
]
# Não informado (not reported), Não se aplica (not applicable), Não disponível (not available) and Sem
# informação mark a missing value in Brazilian data: CAPES writes NÃO SE APLICA in most cells of the
# subarea and speciality of its graduate programmes and NÃO INFORMADO for an unknown organisation, the
# Banco Central do Brasil Não disponível for a payment system closed on Sundays, and the BNDES fills the
# credited bank of a direct operation with ten hyphens (----------).
PORTUGUESE_EMPTY_VALUES = [
    "Não informado", "Não Informado", "não informado", "NÃO INFORMADO",
    "Não se aplica", "Não Se Aplica", "não se aplica", "NÃO SE APLICA",
    "Não disponível", "Não Disponível", "não disponível", "NÃO DISPONÍVEL",
    "Sem informação", "Sem Informação", "sem informação", "SEM INFORMAÇÃO",
    "----------",
]
# A colon is the "not available" of Eurostat tables and of the national tables made for them: the asylum and
# residence permit tables of Cyprus (data.gov.cy) fill 282 218 cells of 50 tables with it in their first
# 1000 lines, and a column of years or counts with a few colons lost its label. Statistical tables also
# write two hyphens, three spaced hyphens, an ellipsis, a lone full stop (the missing value of SAS) or n.a.,
# and Cypriot registers Δ/Ε (δεν εφαρμόζεται, not applicable), alone or as Δ/Ε - N/A.
STATISTICAL_EMPTY_VALUES = [":", "--", "- - -", "…", "...", ".", "n.a.", "N.A.", "n.a"]
GREEK_EMPTY_VALUES = ["Δ/Ε", "Δ/Ε - N/A", "Δ/Ε - Ν/Α"]
# German statistical tables mark a cell without a figure with a lone x (the cell is blocked: a party that
# did not stand in an election, a rate that makes no sense), an asterisk (the figure is withheld for
# statistical confidentiality) or three hyphens: in the first lines of the 1 650 distinct tables of April
# 2024 snapshots of German city portals (Offene Daten Köln, Düsseldorf, Bonn, Münster and others) x stands
# in 141 columns of numbers, the asterisk in 77 and --- in 25. Registers write k. A. or keine Angabe (not
# stated) and ohne Angabe. A capital X is left a value: it is the tick of a checklist column.
GERMAN_EMPTY_VALUES = [
    "x", "*", "---",
    "k. A.", "k.A.", "k. A", "k.A", "K. A.", "K.A.",
    "keine Angabe", "Keine Angabe", "keine Angaben", "Keine Angaben", "KEINE ANGABE",
    "ohne Angabe", "Ohne Angabe", "OHNE ANGABE",
]
# British transparency returns write Nil Return, or Nil, in every cell of a row that reports nothing (no gift,
# no hospitality, no trip in the quarter), and tables of counts replace a small number with <5 or <=5 so that
# no person can be told from it: in the first lines of the 7 107 distinct tables of 2024 snapshots of UK
# open data portals (data.gov.uk, Cambridgeshire Insight, Calderdale Data Works and others) Nil Return stands
# in 1 023 columns (25 264 cells), Nil in 289, <=5 in 3 919 (63 174 cells) and <5 in 3 393 (47 232 cells). Other
# thresholds (<10, <3) are rare and stay values.
BRITISH_EMPTY_VALUES = [
    "Nil Return", "Nil return", "nil return", "NIL RETURN", "NIL Return", "NIL return",
    "Nil", "NIL", "nil",
    "<5", "<=5",
]
DEFAULT_EMPTY_VALUES = (
    [None, "", "None", "NaN", "-", "N/A", "NA"] + NULL_EMPTY_VALUES + SPANISH_EMPTY_VALUES + ENGLISH_EMPTY_VALUES + ARABIC_EMPTY_VALUES
    + PORTUGUESE_EMPTY_VALUES + STATISTICAL_EMPTY_VALUES + GREEK_EMPTY_VALUES + GERMAN_EMPTY_VALUES
    + BRITISH_EMPTY_VALUES
)

DEFAULT_OPTIONS = {
    "encoding": "utf8",
    "delimiter": ",",
    "limit": 1000,
    "empty": DEFAULT_EMPTY_VALUES,
}


def get_file_type(filename: str) -> Optional[str]:
    """Check if file type is supported and return extension.
    
    Args:
        filename: Path to file
        
    Returns:
        File extension (lowercase) if supported, None otherwise
    """
    ext = filename.rsplit(".", 1)[-1].lower()
    if ext in SUPPORTED_FILE_TYPES:
        return ext
    return None


def get_option(options: Dict[str, Any], name: str) -> Any:
    """Get option value from options dict or default options.
    
    Args:
        options: Dictionary of options
        name: Option name to retrieve
        
    Returns:
        Option value from options dict, default options, or None
    """
    if name in options:
        return options[name]
    elif name in DEFAULT_OPTIONS:
        return DEFAULT_OPTIONS[name]
    return None


def guess_int_size(i: int) -> str:
    """Identify the size category of an integer.
    
    Args:
        i: Integer value
        
    Returns:
        Size category: "uint8", "uint16", or "uint32"
    """
    if i < 256:
        return "uint8"
    if i < 65536:
        return "uint16"
    return "uint32"


def guess_datatype(
    value: Any, qd_object: Optional[DateParser] = None
) -> Dict[str, Any]:
    """Guess the data type of a value.
    
    Analyzes a value and determines its base type (str, int, float, bool,
    datetime, date, etc.) and subtype information.
    
    Args:
        value: Value to analyze (can be any type)
        qd_object: Optional DateParser instance for date pattern matching
        
    Returns:
        Dictionary with 'base' key indicating type, and optionally
        'subtype' or 'pat' keys for additional information
    """
    attrs = {"base": "str"}
    #    s = unicode(s)
    if value is None:
        return {"base": "empty"}
    elif isinstance(value, bool):
        return {"base": "bool"}
    elif isinstance(value, int):
        return {"base": "int"}
    elif isinstance(value, float):
        return {"base": "float"}
    elif isinstance(value, datetime):
        return {"base": "datetime"}
    elif isinstance(value, date):
        return {"base": "date"}
    elif not isinstance(value, str):
        #        print((type(s)))
        return {"base": "typed"}
    #    s = s.decode('utf8', 'ignore')
    # Check for empty string before other string operations
    if not value or len(value.strip()) == 0:
        return {"base": "empty"}
    if value.isdigit():
        if value[0] == "0":
            attrs = {"base": "numstr"}
        else:
            attrs = {"base": "int", "subtype": guess_int_size(int(value))}
    else:
        # float() takes the underscores of Python number literals: a code with one
        # (186101_1, a Polish cadastral unit; 2019_2020) is text, not 1861011.0.
        if "_" not in value:
            try:
                temp = float(value)
                attrs = {"base": "float"}
                return attrs
            except ValueError:
                pass
        if qd_object:
            is_date = False
            res = qd_object.match(value)
            if res:
                attrs = {"base": "date", "pat": res["pattern"]}
                is_date = True
    return attrs


from metacrafter.classify.utils import dict_generator


class Analyzer:
    """Analyzer class to process data files and generate field statistics.
    
    Analyzes structured data files (CSV, JSONL, BSON) or in-memory data
    structures and computes statistics for each field including:
    - Data types
    - Uniqueness metrics
    - Length statistics
    - Character composition
    - Dictionary detection
    """
    
    def __init__(self, nodates: bool = True) -> None:
        """Initialize analyzer.
        
        Args:
            nodates: If True, disable date pattern matching (faster).
                If False, enable qddate DateParser for date detection
        """
        if nodates:
            self.qd: Optional[DateParser] = None
        else:
            self.qd: Optional[DateParser] = DateParser(generate=True)

    def analyze(
        self,
        fromfile: Optional[str] = None,
        itemlist: Optional[List[Dict[str, Any]]] = None,
        options: Optional[Dict[str, Any]] = None,
        progress: Optional[Any] = None,
    ) -> Optional[List[List[Any]]]:
        """Analyze data file or list and produce field statistics.

        Processes a data file or in-memory list of dictionaries and computes
        comprehensive statistics for each field including type detection,
        uniqueness, length metrics, and dictionary identification.

        Args:
            fromfile: Optional path to source file (CSV, JSONL, or BSON).
                Mutually exclusive with itemlist
            itemlist: Optional list of dictionaries to process.
                Mutually exclusive with fromfile
            options: Dictionary of analyzer options:
                - encoding: Character encoding (default: "utf8")
                - delimiter: CSV delimiter (default: ",")
                - limit: Maximum records to process (default: 1000)
                - empty: List of values treated as empty
                - format_in: Force file format (overrides auto-detection)
                - zipfile: If True, treat file as ZIP archive
                - dictshare: Dictionary share threshold percentage (default: 10)
            progress: Optional progress bar object with .update(count) method
                that is called once per processed record

        Returns:
            List of lists, where each inner list represents statistics for one field.
            Each row contains: [key, ftype, is_dictkey, is_uniq, n_uniq, share_uniq,
            minlen, maxlen, avglen, tags, has_digit, has_alphas, has_special,
            minval, maxval, has_any_digit, has_any_alphas, has_any_special, dictvalues]
            Returns None if both fromfile and itemlist are None

        Raises:
            IOError: If file cannot be opened or read
            ValueError: If file format is unsupported
        """
        if fromfile is None and itemlist is None:
            return None
        
        if itemlist is not None and len(itemlist) == 0:
            return None

        if options is None:
            options = {}
        
        if "empty" not in options:
            options["empty"] = DEFAULT_EMPTY_VALUES

        dictshare = get_option(options, "dictshare")
        if isinstance(dictshare, int):
            pass
        elif dictshare and dictshare.isdigit():
            dictshare = int(dictshare)
        elif dictshare is None:
            dictshare = DEFAULT_DICT_SHARE
        else:
            dictshare = DEFAULT_DICT_SHARE

        profile = {"version": 1.0}
        fielddata = {}
        fieldtypes = {}

        #    data = json.load(open(profile['filename']))
        count = 0
        nfields = 0

        if fromfile:
            f_type = (
                get_file_type(fromfile)
                if options["format_in"] is None
                else options["format_in"]
            )
            if f_type not in ["jsonl", "bson", "csv"]:
                print("Only JSON lines (.jsonl), .csv and .bson files supported now")
                return

            if 'zipfile' in options and options["zipfile"]:
                z = zipfile.ZipFile(fromfile, mode="r")
                fnames = z.namelist()
                finfilename = fnames[0]
                if f_type == "bson":
                    infile = z.open(fnames[0], "rb")
                else:
                    infile = z.open(fnames[0], "r")
            else:
                finfilename = fromfile
                if f_type == "bson":
                    infile = open(fromfile, "rb")
                else:
                    infile = open(
                        fromfile, "r", encoding=get_option(options, "encoding")
                    )

            # Identify item list
            itemlist = []

            if f_type == "jsonl":
                for l in infile:
                    itemlist.append(orjson.loads(l))
            elif f_type == "csv":
                delimiter = get_option(options, "delimiter")
                reader = csv.DictReader(infile, delimiter=delimiter)
                for r in reader:
                    itemlist.append(r)
            elif f_type == "bson":
                bson = require_extra("bson", "mongodb", "Reading BSON files")
                bson_iter = bson.decode_file_iter(infile)
                for r in bson_iter:
                    itemlist.append(r)

        # process data items one by one
        if fromfile is not None:
            logging.debug("Started analyzing %s", fromfile)
        elif itemlist is not None:
            logging.debug("Started analyzing array with %d records", len(itemlist))
        for item in itemlist:
            count += 1
            if progress is not None:
                progress.update(1)
            dk = dict_generator(item)
            if count % 1000 == 0:
                logging.debug("Processing %d records of %s", count, fromfile)
            for i in dk:
                #            print(i)
                k = ".".join(i[:-1])
                if len(i) == 0:
                    continue
                v = i[-1]
                if k not in fielddata:
                    fielddata[k] = {
                        "key": k,
                        "uniq": {},
                        "n_uniq": 0,
                        "total": 0,
                        "share_uniq": 0.0,
                        "minlen": None,
                        "maxlen": 0,
                        "avglen": 0,
                        "totallen": 0,
                        "has_digit": 0,
                        "has_alphas": 0,
                        "has_special": 0,
                        "minval": None,
                        "maxval": None,
                    }
                fd = fielddata[k]
                val_s = str(v)
                uniqval = fd["uniq"].get(val_s, 0)
                fd["uniq"][val_s] = uniqval + 1
                fd["total"] += 1
                if uniqval == 0:
                    fd["n_uniq"] += 1
                    fd["share_uniq"] = (fd["n_uniq"] * 100.0) / fd["total"]
                fl = len(str(v))
                if fd["minlen"] is None:
                    fd["minlen"] = fl
                else:
                    fd["minlen"] = fl if fl < fd["minlen"] else fd["minlen"]
                fd["maxlen"] = fl if fl > fd["maxlen"] else fd["maxlen"]
                fd["totallen"] += fl
                fielddata[k] = fd
                if k not in fieldtypes:
                    fieldtypes[k] = {"key": k, "types": {}}
                fd = fieldtypes[k]
                thetype = guess_datatype(v, self.qd)["base"]
                # Check character composition for string values (including numeric strings)
                val_str = str(v)
                if isinstance(v, str) and len(val_str) > 0:
                    fielddata[k]["has_digit"] += (
                        1 if any(char.isdigit() for char in val_str) else 0
                    )
                    fielddata[k]["has_alphas"] += (
                        1 if any(char.isalpha() for char in val_str) else 0
                    )
                    fielddata[k]["has_special"] += (
                        1 if any(not char.isalnum() and not char.isspace() for char in val_str) else 0
                    )
                # Track min/max values for numeric fields
                if thetype in ["int", "float"]:
                    try:
                        num_val = float(v) if isinstance(v, str) else v
                        if isinstance(num_val, (int, float)):
                            if fielddata[k]["minval"] is None:
                                fielddata[k]["minval"] = num_val
                                fielddata[k]["maxval"] = num_val
                            else:
                                fielddata[k]["minval"] = min(fielddata[k]["minval"], num_val)
                                fielddata[k]["maxval"] = max(fielddata[k]["maxval"], num_val)
                    except (ValueError, TypeError):
                        pass
                uniqval = fd["types"].get(thetype, 0)
                fd["types"][thetype] = uniqval + 1
                fieldtypes[k] = fd
        #        print count
        for k, v in list(fielddata.items()):
            fielddata[k]["share_uniq"] = (v["n_uniq"] * 100.0) / v["total"]
            fielddata[k]["avglen"] = v["totallen"] / v["total"]
            # Convert counts to booleans for character composition
            fielddata[k]["has_any_digit"] = v["has_digit"] > 0
            fielddata[k]["has_any_alphas"] = v["has_alphas"] > 0
            fielddata[k]["has_any_special"] = v["has_special"] > 0
        profile["count"] = count
        profile["num_fields"] = nfields
        finfields = {}
        for k, v in list(fielddata.items()):
            #            del v['uniq']
            fielddata[k] = v
        for fdk in list(fieldtypes.values()):
            fdt = list(fdk["types"].keys())
            if "empty" in fdt:
                del fdk["types"]["empty"]
            if len(list(fdk["types"].keys())) != 1:
                ftype = "str"
            else:
                ftype = list(fdk["types"].keys())[0]
            finfields[fdk["key"]] = ftype

        dictkeys = []
        dicts = {}
        #        print(profile)
        profile["fields"] = []
        for fd in list(fielddata.values()):
            #            print(fd)
            #            print(fd['key'])  # , fd['n_uniq'], fd['share_uniq'], fieldtypes[fd['key']]
            field = {"key": fd["key"], "is_uniq": 0 if fd["share_uniq"] < 100 else 1}
            profile["fields"].append(field)
            if fd["share_uniq"] <= dictshare:
                dictkeys.append(fd["key"])
                dicts[fd["key"]] = {
                    "items": fd["uniq"],
                    "count": fd["n_uniq"],
                    "total": sum(fd["uniq"].values()),
                    "type": finfields[fd["key"]],  # Type is determined by guess_datatype()
                }
        #            for k, v in fd['uniq'].items():
        #                print fd['key'], k, v
        profile["dictkeys"] = dictkeys

        profile["debug"] = {"fieldtypes": fieldtypes.copy(), "fielddata": fielddata}
        profile["fieldtypes"] = finfields
        table = []
        for fd in list(fielddata.values()):
            field = [
                fd["key"],
            ]
            field.append(finfields[fd["key"]])
            field.append(fd["key"] in dictkeys)
            field.append(fd["share_uniq"] >= 100)
            field.append(fd["n_uniq"])
            field.append(fd["share_uniq"])
            field.append(fd["minlen"])
            field.append(fd["maxlen"])
            field.append(fd["avglen"])
            tags = []
            if fd["share_uniq"] == 100:
                tags.append("uniq")
            allempty = 0
            if fd["key"] in dicts:
                empty_values = options.get("empty", DEFAULT_EMPTY_VALUES)
                if empty_values is None:
                    empty_values = DEFAULT_EMPTY_VALUES
                for key, value in dicts[fd["key"]]["items"].items():
                    if key in empty_values:
                        allempty += value
                if allempty == dicts[fd["key"]]["total"]:
                    tags.append("empty")
                else:
                    tags.append("dict")
            field.append(tags)
            field.append(fd["has_digit"])
            field.append(fd["has_alphas"])
            field.append(fd["has_special"])
            field.append(fd["minval"])
            field.append(fd["maxval"])
            field.append(fd["has_any_digit"])
            field.append(fd["has_any_alphas"])
            field.append(fd["has_any_special"])
            field.append(list(fd["uniq"].keys()) if fd["key"] in dictkeys else None)
            table.append(field)
        return table

    def print(self, table: List[List[Any]]) -> None:
        """Print analysis table in formatted form.
        
        Args:
            table: List of field statistics rows (as returned by analyze())
        """
        from tabulate import tabulate

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
        ]
        print(tabulate(table, headers=headers))

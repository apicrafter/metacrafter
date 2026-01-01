# -*- coding: utf-8 -*-
"""Statistics module for analyzing data files and generating field statistics."""
import csv
import logging
import zipfile
from datetime import datetime, date
from typing import List, Dict, Any, Optional, Union, Generator, Tuple

import bson
import orjson
from qddate import DateParser

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

DEFAULT_EMPTY_VALUES = [None, "", "None", "NaN", "-", "N/A"]

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


def dict_generator(
    indict: Union[Dict[str, Any], Any], pre: Optional[List[str]] = None
) -> Generator[List[Any], None, None]:
    """Generate flattened key-value pairs from nested dictionary.
    
    Recursively traverses nested dictionaries and lists, generating
    dot-separated key paths with their values. Skips MongoDB _id fields.
    
    Args:
        indict: Dictionary or value to process
        pre: Prefix list for nested keys (used in recursion)
        
    Yields:
        Lists of [key_path..., value] where key_path is a list of keys
    """
    pre = pre[:] if pre else []
    if isinstance(indict, dict):
        for key, value in list(indict.items()):
            if key == "_id":
                continue
            if isinstance(value, dict):
                #                print 'dgen', value, key, pre
                for d in dict_generator(value, pre + [key]):
                    yield d
            elif isinstance(value, list) or isinstance(value, tuple):
                for v in value:
                    if isinstance(v, dict):
                        #                print 'dgen', value, key, pre
                        for d in dict_generator(v, pre + [key]):
                            yield d
            #                    for d in dict_generator(v, [key] + pre):
            #                        yield d
            else:
                yield pre + [key, value]
    else:
        yield indict


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
                if i[0].isdigit():
                    continue
                if len(i[0]) == 1:
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

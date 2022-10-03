# -*- coding: utf-8 -*-
"""Statistics module"""
import csv
import logging
import zipfile
from datetime import datetime, date

import bson
import orjson
from qddate import DateParser

DEFAULT_DICT_SHARE = 10
SUPPORTED_FILE_TYPES = [
    "xls",
    "xlsx",
    "csv",
    "xml",
    "json",
    "jsonl",
    "yaml",
    "tsv",
    "sql",
    "bson",
]

DEFAULT_EMPTY_VALUES = [None, "", "None", "NaN", "-", "N/A"]

DEFAULT_OPTIONS = {
    "encoding": "utf8",
    "delimiter": ",",
    "limit": 1000,
    "empty": DEFAULT_EMPTY_VALUES,
}


def get_file_type(filename):
    """Returns is file type supported"""
    ext = filename.rsplit(".", 1)[-1].lower()
    if ext in SUPPORTED_FILE_TYPES:
        return ext
    return None


def get_option(options, name):
    """Returns value of the option"""
    if name in options.keys():
        return options[name]
    elif name in DEFAULT_OPTIONS:
        return DEFAULT_OPTIONS[name]
    return None


def guess_int_size(i):
    """Identifies size of the integer"""
    if i < 255:
        return "uint8"
    if i < 65535:
        return "uint16"
    return "uint32"


def guess_datatype(value, qd_object=None):
    """Guesses type of data by string provided"""
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
            if not is_date:
                if len(value.strip()) == 0:
                    attrs = {"base": "empty"}
    return attrs


def dict_generator(indict, pre=None):
    """Generates keys from dictionary"""
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
    """Analyzer class to process data files and generate stats"""
    def __init__(self, nodates=True):
        if nodates:
            self.qd = None
        else:
            self.qd = DateParser(generate=True)
        pass

    def analyze(self, fromfile=None, itemlist=None, options=DEFAULT_OPTIONS):
        """Analyzes JSON or another data file and produces stats"""
        if fromfile == None and itemlist == None:
            return None

        if "empty" not in options.keys():
            options["empty"] = DEFAULT_EMPTY_VALUES

        dictshare = get_option(options, "dictshare")
        if dictshare and dictshare.isdigit():
            dictshare = int(dictshare)
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

            if options["zipfile"]:
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
        logging.debug("Start processing %s" % (fromfile))
        for item in itemlist:
            count += 1
            dk = dict_generator(item)
            if count % 1000 == 0:
                logging.debug("Processing %d records of %s" % (count, fromfile))
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
                if k not in list(fielddata.keys()):
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
                if k not in list(fieldtypes.keys()):
                    fieldtypes[k] = {"key": k, "types": {}}
                fd = fieldtypes[k]
                thetype = guess_datatype(v, self.qd)["base"]
                if thetype == "str":
                    fielddata[k]["has_digit"] += (
                        1 if any(char.isdigit() for char in v) else 0
                    )
                    fielddata[k]["has_alphas"] += (
                        1 if any(char.isalpha() for char in v) else 0
                    )
                    fielddata[k]["has_special"] += (
                        1 if any(not char.isalnum() for char in v) else 0
                    )
                uniqval = fd["types"].get(thetype, 0)
                fd["types"][thetype] = uniqval + 1
                fieldtypes[k] = fd
        #        print count
        for k, v in list(fielddata.items()):
            fielddata[k]["share_uniq"] = (v["n_uniq"] * 100.0) / v["total"]
            fielddata[k]["avglen"] = v["totallen"] / v["total"]
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
            if fd["share_uniq"] < dictshare:
                dictkeys.append(fd["key"])
                dicts[fd["key"]] = {
                    "items": fd["uniq"],
                    "count": fd["n_uniq"],
                    "total": sum(fd["uniq"].values()),
                    "type": finfields[fd["key"]],
                }  # TODO: Shouldn't be "str" by default
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
            field.append(True if fd["key"] in dictkeys else False)
            field.append(False if fd["share_uniq"] < 100 else True)
            field.append(fd["n_uniq"])
            field.append(fd["share_uniq"])
            field.append(fd["minlen"])
            field.append(fd["maxlen"])
            field.append(fd["avglen"])
            tags = []
            if fd["share_uniq"] == 100:
                tags.append("uniq")
            allempty = 0
            if fd["key"] in dicts.keys():
                for key, value in dicts[fd["key"]]["items"].items():
                    if key in options["empty"]:
                        allempty += value
                if allempty == dicts[fd["key"]]["total"]:
                    tags.append("empty")
                else:
                    tags.append("dict")
            field.append(tags)
            field.append(fd["has_digit"])
            field.append(fd["has_alphas"])
            field.append(fd["has_special"])
            field.append(list(fd["uniq"].keys()) if fd["key"] in dictkeys else None)
            table.append(field)
        return table

    def print(self, table):
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

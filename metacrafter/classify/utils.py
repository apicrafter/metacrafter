# -*- coding: utf-8 -*-
"""Utility functions that help to work with data"""
import chardet
from collections import defaultdict, OrderedDict
import xmltodict


def dict_generator(indict, pre=None):
    """Generates schema from dictionary object"""
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


def headers(data, limit=1000):
    """Returns headers of list of dict objects"""
    iter_num = 0
    keys = []
    for item in data:
        iter_num += 1
        if iter_num > limit:
            break
        dict_gen = dict_generator(item)
        for i in dict_gen:
            k = ".".join(i[:-1])
            if k not in keys:
                keys.append(k)
    return keys


def get_dict_value(object, keys):
    """Returns single value from dictionary object"""
    out = []
    if object is None:
        return out
    #    keys = key.split('.')
    if len(keys) == 1:
        if isinstance(object, dict):
            if keys[0] in object.keys():
                out.append(object[keys[0]])
        else:
            for record in object:
                if record and keys[0] in record.keys():
                    out.append(record[keys[0]])
    #        return out
    else:
        if isinstance(object, dict):
            if keys[0] in object.keys():
                out.extend(get_dict_value(object[keys[0]], keys[1:]))
        else:
            for record in object:
                if keys[0] in record.keys():
                    out.extend(get_dict_value(record[keys[0]], keys[1:]))
    return out


def dict_to_columns(data):
    """Converts list of dictionary objects to list of columns"""
    columns = {}
    for row in data:
        dk = dict_generator(row)
        for i in dk:
            k = ".".join(i[:-1])
            if k in columns.keys():
                columns[k].append(i[-1])
            else:
                columns[k] = [
                    i[-1],
                ]
    return columns


def string_to_charrange(s):
    """Returns array of chars from string"""
    chars = {}
    for ch in s:
        v = chars.get(ch, 0)
        chars[ch] = v + 1
    return chars


def string_array_to_charrange(sarr):
    """Returns char map from array"""
    chars = {}
    for s in sarr:
        for ch in s:
            v = chars.get(ch, 0)
            chars[ch] = v + 1
    return chars


def detect_encoding(filename, limit=1000000):
    """Detects encoding of the filename.
    
    Args:
        filename: Path to the file to analyze
        limit: Maximum bytes to read for detection (default: 1MB)
        
    Returns:
        Dictionary with encoding detection results from chardet
        
    Raises:
        IOError: If file cannot be read
    """
    with open(filename, "rb") as f:
        chunk = f.read(limit)
    detected = chardet.detect(chunk)
    return detected


def detect_delimiter(filename, encoding="utf8"):
    """Detects CSV file delimiter by analyzing first line.
    
    Args:
        filename: Path to the CSV file
        encoding: File encoding (default: utf8)
        
    Returns:
        Most likely delimiter character (',', ';', '\t', or '|')
        
    Raises:
        IOError: If file cannot be read
    """
    with open(filename, "r", encoding=encoding) as f:
        line = f.readline()
    dict1 = {
        ",": line.count(","),
        ";": line.count(";"),
        "\t": line.count("\t"),
        "|": line.count("|"),
    }
    delimiter = max(dict1, key=dict1.get)
    return delimiter


def etree_to_dict(t, prefix_strip=True):
    """Lxml etree converted to Python dictionary for JSON serialization"""
    tag = t.tag if not prefix_strip else t.tag.rsplit("}", 1)[-1]
    d = {tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            #            print(dir(dc))
            for k, v in dc.items():
                if prefix_strip:
                    k = k.rsplit("}", 1)[-1]
                dd[k].append(v)
        d = {tag: {k: v[0] if len(v) == 1 else v for k, v in dd.items()}}
    if t.attrib:
        d[tag].update(("@" + k.rsplit("}", 1)[-1], v) for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            tag = tag.rsplit("}", 1)[-1]
            if text:
                d[tag]["#text"] = text
        else:
            d[tag] = text
    return d


def _seek_xml_lists(data, level=0, path=None, candidates=OrderedDict()):
    """Seeks XML lists to find items tags"""
    for key, value in data.items():
        if isinstance(value, list):
            key = path + ".%s" % (key) if path is not None else key
            if key not in candidates.keys():
                candidates[key] = {"key": key, "num": len(value)}
        elif isinstance(value, OrderedDict) or isinstance(value, dict):
            res = _seek_xml_lists(
                value, level + 1, path + "." + key if path else key, candidates
            )
            for k, v in res.items():
                if k not in candidates.keys():
                    candidates[k] = v
        else:
            continue
    return candidates


def xml_quick_analyzer(filename):
    """Analyzes single XML file and returns tag objects.
    
    Args:
        filename: Path to the XML file
        
    Returns:
        Dictionary with 'full' and 'short' tag keys, or None if no candidates found
        
    Raises:
        IOError: If file cannot be read
        xml.parsers.expat.ExpatError: If XML is malformed
    """
    with open(filename, "rb") as f:
        data = xmltodict.parse(f, process_namespaces=False)
    candidates = _seek_xml_lists(data, level=0)
    if len(candidates) > 0:
        fullkey = str(list(candidates.keys())[0])
        shortkey = fullkey.rsplit(".", 1)[-1]
        if len(shortkey.split(":")) > 0:
            shortkey = shortkey.rsplit(":", 1)[-1]
        return {"full": fullkey, "short": shortkey}
    return None

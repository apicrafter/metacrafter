# -*- coding: utf-8 -*-
import chardet
from collections import defaultdict
from collections import OrderedDict
import xmltodict


def dict_generator(indict, pre=None):
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
    n = 0
    keys = []
    for item in data:
        n += 1
        if n > limit:
            break
        dk = dict_generator(item)
        for i in dk:
            k = ".".join(i[:-1])
            if k not in keys:
                keys.append(k)
    return keys


def get_dict_value(d, keys):
    out = []
    if d is None:
        return out
    #    keys = key.split('.')
    if len(keys) == 1:
        if type(d) == type({}):
            if keys[0] in d.keys():
                out.append(d[keys[0]])
        else:
            for r in d:
                if r and keys[0] in r.keys():
                    out.append(r[keys[0]])
    #        return out
    else:
        if type(d) == type({}):
            if keys[0] in d.keys():
                out.extend(get_dict_value(d[keys[0]], keys[1:]))
        else:
            for r in d:
                if keys[0] in r.keys():
                    out.extend(get_dict_value(r[keys[0]], keys[1:]))
    return out


def dict_to_columns(data):
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
    chars = {}
    for ch in s:
        v = chars.get(ch, 0)
        chars[ch] = v + 1
    return chars


def string_array_to_charrange(sarr):
    chars = {}
    for s in sarr:
        for ch in s:
            v = chars.get(ch, 0)
            chars[ch] = v + 1
    return chars


def detect_encoding(filename, limit=1000000):
    f = open(filename, "rb")
    chunk = f.read(limit)
    f.close()
    detected = chardet.detect(chunk)
    return detected


def detect_delimiter(filename, encoding="utf8"):
    f = open(filename, "r", encoding=encoding)
    line = f.readline()
    f.close()
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
    """Analyzes single XML file and returns tag objects"""
    f = open(filename, "rb")  # , encoding=encoding)
    data = xmltodict.parse(f, process_namespaces=False)
    f.close()
    candidates = _seek_xml_lists(data, level=0)
    if len(candidates) > 0:
        fullkey = str(list(candidates.keys())[0])
        shortkey = fullkey.rsplit(".", 1)[-1]
        if len(shortkey.split(":")) > 0:
            shortkey = shortkey.rsplit(":", 1)[-1]
        return {"full": fullkey, "short": shortkey}
    return None

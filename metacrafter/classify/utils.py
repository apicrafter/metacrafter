import chardet

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
        if n > limit: break
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
                columns[k] = [i[-1],]
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
    f = open(filename, 'rb')
    chunk = f.read(limit)
    f.close()
    detected = chardet.detect(chunk)
    return detected

def detect_delimiter(filename, encoding='utf8'):
    f = open(filename, 'r', encoding=encoding)
    line = f.readline()
    f.close()
    dict1 = {',': line.count(','), ';': line.count(';'), '\t': line.count('\t'), '|' : line.count('|')}
    delimiter = max(dict1, key=dict1.get)
    return delimiter
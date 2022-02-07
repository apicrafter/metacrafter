import logging

from pyparsing import Literal, Optional, ParseException, White, Word, alphanums, alphas, hexnums, lineEnd, lineStart, nums, oneOf, CaselessLiteral, Group, OneOrMore, Combine, printables
import yaml
import glob
import importlib
from pathlib import Path

from .utils import headers, get_dict_value, dict_to_columns

DEFAULT_MAX_LEN = 100
DEFAULT_MIN_LEN = 3

RULE_TYPE_FIELD = 1
RULE_TYPE_DATA = 2

BASE_URL = 'https://meta.apicrafter.io/class/{dataclass}'


class TableScanResult:
    """Results of table scan classification"""
    def __init__(self, results=[]):
        self.results = results
        pass

    def add(self, result):
        """Add one more Columnt scan result"""
        self.results.append(result)

    def is_empty(self):
        """True if empty"""
        return len(self.results) == 0

    def asdict(self):
        res = []
        for m in self.results:
            res.append(m.asdict())
        return res


class ColumnMatchResult:
    """Results of the column classficiations """
    def __init__(self, field, matches=[]):
        self.field = field
        self.matches = matches
        pass

    def add(self, match):
        """Add one more rule result"""
        self.matches.append(match)

    def is_empty(self):
        """True if empty"""
        return len(self.matches) == 0

    def asdict(self):
        matches = []
        for m in self.matches:
            matches.append(m.asdict())
        return {'field' : self.field, 'matches' : matches}



class RuleResult:
    """Result match error"""
    def __init__(self, ruleid, dataclass, confidence, ruletype, piiclass=None, format=None):
        self.ruleid = ruleid
        self.dataclass = dataclass
        self.confidence = confidence
        self.ruletype = ruletype
        self.piiclass = piiclass
        self.format = format

    def is_pii(self):
        return self.piiclass is not None

    def class_url(self):
        return BASE_URL.format(dataclass=self.dataclass)

    def pii_url(self):
        return BASE_URL.format(dataclass=self.piiclass) if self.piiclass is not None else None

    def asdict(self):
        return {'ruleid' : self.ruleid, 'dataclass' : self.dataclass, 'confidence' : self.confidence, 'ruletype' : self.ruletype, 'format' : self.format, 'classurl' : self.class_url()}


class RulesProcessor:
    """Classification rules processor class"""
    def __init__(self, langs=None, contexts=None):
        self.preset_langs = langs
        self.preset_contexts = contexts
        self.reset_rules()
        pass

    def reset_rules(self):
        """Cleans up imported rules."""
        self.data_rules = []
        self.field_rules = []
        self.__rule_keys = []
        self.langs = {}
        self.contexts = {}


    def import_rules(self, filename):
        """Import rules from file"""
        f = open(filename, 'r', encoding='utf8')
        ruledata = yaml.load(f, Loader=yaml.FullLoader)
        f.close()

        # If group of rules context or lang not in allowed list, skip it
        if self.preset_langs and ruledata['lang'] not in self.preset_langs:
            return
        if self.preset_contexts and ruledata['context'] not in self.preset_contexts:
            return

        for rulekey in ruledata['rules'].keys():
            if rulekey in self.__rule_keys:
                continue
            else:
                self.__rule_keys.append(rulekey)
            rule = ruledata['rules'][rulekey]

            rule['imprecise'] = (int(rule['imprecise']) != 0) if 'imprecise' in rule.keys() else False
#            print(rulekey, rule['imprecise'])
            if rule['match'] == 'ppr':
                rule['compiled'] = lineStart + eval(rule['rule']) + lineEnd
            elif rule['match'] == 'func':
                module, funcname = rule['rule'].rsplit('.', 1)
                match_func = getattr(importlib.import_module(module), funcname)
                rule['compiled'] = match_func
            elif rule['match'] == 'text':
                keywords = rule['rule'].split(',')
                ruledata['rules'][rulekey]['compiled'] = lineStart + oneOf(keywords, caseless=True) + lineEnd
                ruledata['rules'][rulekey]['keywords'] = list(map(str.lower, keywords))
            if rule['match'] == 'text':
                rule['maxlen'] = len(max(keywords, key=len))
                rule['minlen'] = len(min(keywords, key=len))
            else:
                rule['maxlen'] = int(rule['maxlen']) if 'maxlen' in rule.keys() else DEFAULT_MAX_LEN
                rule['minlen'] = int(rule['minlen']) if 'minlen' in rule.keys() else DEFAULT_MIN_LEN
            if 'validator' in rule.keys():
                module, funcname = rule['validator'].rsplit('.', 1)
                match_func = getattr(importlib.import_module(module), funcname)
                rule['vfunc'] = match_func
            if 'fieldrule' in rule.keys() and 'fieldrulematch' in rule.keys():
                if rule['fieldrulematch'] == 'ppr':
                    rule['f_compiled'] = lineStart + eval(rule['fieldrule']) + lineEnd
                elif rule['fieldrulematch'] == 'text':
                    keywords = rule['fieldrule'].split(',')
                    ruledata['rules'][rulekey]['fieldkeywords'] = list(map(str.lower, keywords))
                    ruledata['rules'][rulekey]['f_compiled'] = lineStart + oneOf(keywords, caseless=True) + lineEnd
            rule['id'] = rulekey
            for key in ['context', 'lang']:
                rule[key] = ruledata[key]
            rule['group'] = ruledata['name']
            rule['group_desc'] = ruledata['description']
            if rule['type'] == 'field':
                self.field_rules.append(rule)
            elif rule['type'] == 'data':
                self.data_rules.append(rule)

            v = self.langs.get(rule['lang'], 0)
            self.langs[rule['lang']] = v + 1

            contexts = rule['context'].split('.')
            rule['context'] = contexts

            # Add more than one context
            if 'piikey' in rule.keys() and 'pii' not in rule['context']:
                rule['context'].append('pii')

            for context in contexts:
                v = self.contexts.get(context, 0)
                self.contexts[context] = v + 1


        logging.debug('Loaded rules from %s' % filename)


    def import_rules_path(self, pathname, recursive=True):
        """Import rules from path"""
        if not recursive:
            filenames = glob.glob(pathname + '/*.yaml')
            for filename in filenames:
                self.import_rules(filename)
        else:
            for path in Path(pathname).rglob('*.yaml'):
                self.import_rules(str(path))

    def dumpStats(self):
        """Dump statistics"""
        import qddate

        print('Rule types:')
        print('- field based rules %d' % (len(self.field_rules)))
        print('- data based rules %d' % (len(self.data_rules)))
        print('Context:')
        for key in sorted(self.contexts.keys()):
            print('- %s %d' % (key, self.contexts[key]))
        print('Language:')
        for key in sorted(self.langs.keys()):
            print('- %s %d' % (key, self.langs[key]))
        dparser = qddate.DateParser(patterns=qddate.patterns.PATTERNS_EN + qddate.patterns.PATTERNS_RU)
        print('Data/time patterns (qddate): %d' % (len(dparser.patterns)))

    def get_filtered_rules(self, ruletype, contexts=None, langs=None, ignore_imprecise=False):
        """Filters rules by ruletype, context and languages. Helps to filter rules before data matching"""
        rules = self.field_rules if ruletype == RULE_TYPE_FIELD else self.data_rules
        filtered = []
        if not contexts and not langs and not ignore_imprecise:
            return rules
        for rule in rules:
            in_context = False
            in_lang = False
            in_imprecise = False
            if not contexts:
                in_context = True
            else:
                for rule_context in rule['context']:
                    if rule_context in contexts:
                        in_context = True
                        break
            if not langs:
                in_lang = True
            elif rule['lang'] in langs:
                in_lang = True
            if ignore_imprecise and rule['imprecise']:
                in_imprecise = True
            if in_lang and in_context and not in_imprecise:
                filtered.append(rule)
#            else:
#                print('Rule %s removed' % (rule['key']))
#            print(rule['key'], in_lang, in_context, in_imprecise)
        return filtered

    def match_dict(self, data, fields=None, datastats=None, confidence=95, stop_on_match=False, parse_dates=True, dateparser=None, limit=1000, filter_contexts=None, filter_langs=None, except_empty=True, ignore_imprecise=True):
        """Matches python array of dicts (from JSON lines or BSON)"""
        results = TableScanResult()
        if not fields:
            fields = headers(data)
        field_rules = self.get_filtered_rules(RULE_TYPE_FIELD, filter_contexts, filter_langs, ignore_imprecise)
        data_rules = self.get_filtered_rules(RULE_TYPE_DATA, filter_contexts, filter_langs, ignore_imprecise)

        data_columns = dict_to_columns(data)
        nonstr = []
        if datastats:
            for field in datastats.keys():
                if datastats[field]['ftype'] != 'str':
                    nonstr.append(field)
        for field in fields:
            m_result = ColumnMatchResult(field=field, matches=[])
            shortfield = field.rsplit('.', 1)[-1].strip()
            # Match field name first
            for rule in field_rules:
                if rule['match'] == 'func':
                    res = rule['compiled'](shortfield)
                    if res:
                        m_result.add(RuleResult(ruleid=rule['id'], confidence=100, dataclass=rule['key'], ruletype='field'))
                        if stop_on_match:
                            break
                elif rule['match'] == 'ppr':
                    try:
                        res = rule['compiled'].parseString(shortfield)
                        m_result.add({'dataclass': rule['key'], 'confidence': 100, 'ruleid' : rule['id'], 'ruletype' : 'field'})
                        if stop_on_match:
                            break
                    except ParseException:
                        pass
                elif rule['match'] == 'text':
                    if shortfield.lower() in rule['keywords']:
                        m_result.add(RuleResult(ruleid=rule['id'], confidence=100, dataclass=rule['key'], ruletype='field'))
                        if stop_on_match:
                            break
            data = data_columns[field]
            slice = data[0:limit]
            min_len = 0
            max_len = 0
            if datastats:
                # Match boolean as bool type and ignore float fields right now #FIXME
                if field in datastats.keys():
                    if datastats[field]['ftype'] == 'bool':
                        m_result.add(RuleResult(ruleid='_int_fieldtype_boolean', confidence=100, dataclass='boolean', ruletype='fieldtype'))
                        results.add(m_result)
                        continue
                    elif datastats[field]['ftype'] == 'float':
                        results.add(m_result)
                        continue
                    elif datastats[field]['ftype'] == 'datetime':
                        m_result.add(RuleResult(ruleid='_int_fieldtype_datetime', confidence=100, dataclass='datetime', ruletype='fieldtype'))
                        results.add(m_result)
                        continue
                    elif datastats[field]['ftype'] == 'date':
                        m_result.add(RuleResult(ruleid='_int_fieldtype_date', confidence=100, dataclass='date', ruletype='fieldtype'))
                        results.add(m_result)
                        continue
                    min_len = datastats[field]['minlen']
                    max_len = datastats[field]['maxlen']
            if min_len == 0:
                try:
                    min_len = len(min(data, key=len))
                    max_len = len(max(data, key=len))
                except TypeError:
                    min_len = 4
                    max_len = 4

#            if field not in nonstr:

            if field:
                # Filtering rules by field max and min length of the data
                rules = []
                for rule in data_rules:
#                    print('Field %s %d %d, rule %s %d %d' % (field, min_len, max_len, rule['key'], rule['minlen'], rule['maxlen']))
                    if (min_len >= rule['minlen'] and min_len <= rule['maxlen']) or (rule['minlen'] >= min_len and rule['minlen'] <= max_len):
                        if 'f_compiled' in rule.keys():
                                if rule['fieldrulematch'] == 'ppr':
                                    try:
                                        res = rule['f_compiled'].parseString(shortfield)
                                        rules.append(rule)
                                    except ParseException:
                                        pass
                                elif rule['fieldrulematch'] == 'text':
                                    if shortfield.lower() in rule['fieldkeywords']:
                                        rules.append(rule)
                        else:
                            rules.append(rule)
#                print(field)
#                for rule in rules:
#                    print('- %s' %(rule['key']))
                for rule in rules:
                    success = 0
                    empty = 0
                    total = len(slice)
                    for value in slice:
                        if not value:
                            if except_empty:
                                empty += 1
                            continue
#                        if not isinstance(value, str): continue
                        slen = len(str(value))
                        if slen == 0:
                            if except_empty:
                                empty += 1
                            continue

                        if slen < rule['minlen'] or slen > rule['maxlen']: continue
                        if rule['match'] == 'func':
                            try:
                                res = rule['compiled'](str(value))
                                if res:
                                    success += 1
                            except KeyboardInterrupt:
                                pass
                        elif rule['match'] == 'ppr':
                            try:
                                res = rule['compiled'].parseString(str(value))
                                if 'vfunc' in rule.keys():
                                    isvalid = rule['vfunc'](str(value))
                                    if isvalid:
                                        success += 1
                                else:
                                    success += 1
                            except ParseException as e:
                                pass
                        elif rule['match'] == 'text':
                            if str(value).lower() in rule['keywords']:
                                success += 1
                    if except_empty:
                        subtotal = total - empty
                        if subtotal == 0:
                            result = 0
                        else:
                            result = (success * 100.0 / (total - empty))
                    else:
                        result = success * 100.0 / total
                    if result > confidence:
                        m_result.add(RuleResult(ruleid=rule['id'], confidence=result, dataclass=rule['key'], ruletype='data'))
                        if stop_on_match:
                            break

            if m_result.is_empty() and parse_dates and field not in nonstr:
                total = len(slice)
                success = 0
                empty = 0
                date_format = None
                for value in slice:
                    if value is None:
                        if except_empty:
                            empty += 1
                        continue
                    if len(str(value)) == 0:
                        if except_empty:
                            empty += 1
                        continue
                    if not isinstance(value, str): continue
                    res = dateparser.match(value, noyear=False)
                    if res:
                        success += 1
                        date_format = res['pattern']['key']

                if except_empty:
                    subtotal = total - empty
                    if subtotal == 0:
                        result = 0
                    else:
                        result = (success * 100.0 / (total - empty))
                else:
                    result = success * 100.0 / total
                if result > confidence:
                    m_result.add(
                        RuleResult(ruleid='qddate', confidence=result, dataclass='datetime', ruletype='data', format=date_format))
            results.add(m_result)
        return results

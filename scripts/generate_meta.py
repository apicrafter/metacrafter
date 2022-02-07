#!/usr/bin/env python
import typer
from metacrafter.classify.processor import RulesProcessor
import yaml

RULEPATHS = [
			'..\/rules', 
			'..\/..\/metacrafter-rules\/rules'
		]



def generate():
    identifiers = {}
    
    typer.echo(f"Processing rules")
    pr = RulesProcessor()
    for rp in RULEPATHS:
        pr.import_rules_path(rp, recursive=True)
    for rules in [pr.data_rules, pr.field_rules]:
        for rule in rules:
            for k in ['key', 'piikey']:
                if k not in rule.keys(): continue
                if rule[k] not in identifiers.keys():
                    identifiers[rule[k]] = {'id' : rule[k], 'is_pii' : True if 'piikey' in rule.keys() else False, 'contexts' : rule['context'], 'langs' : [rule['lang']], 'name' : '', 'doc' : "", "translations" : {'ru' : {'name' : "", "doc" : ""}}, "links" : [{"type" : "", "url" : ""}]}
                else:
                    for context in rule['context']:
                        if context not in identifiers[rule[k]]['contexts']:
                            identifiers[rule[k]]['contexts'].append(context)                   
                    if rule['lang'] not in identifiers[rule[k]]['langs']:
                        identifiers[rule[k]]['langs'].append(rule['lang'])
    print(yaml.dump(identifiers))
                    
#    for k in sorted(identifiers.keys()):
#        print(identifiers[k])    

if __name__ == "__main__":
    typer.run(generate)
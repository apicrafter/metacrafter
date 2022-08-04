# Metacrafter

Python command line tool and python engine to label table fields and fields in data files.
It could help to find meaningful data in your tables and data files or to find Personal identifable information (PII).


## Installation

To install Python library use `pip install metacrafter` via pip or `python setup.py install` 

## Features

Metacrafter is a rule based tool that helps to label fields of the tables in databases. It scans table and finds person names, surnames, midnames, PII data, basic identifiers like UUID/GUID. 
These rules written as .yaml files and could be easily extended.

File formats supported:
* CSV
* JSON lines
* JSON (array of records)
* BSON
* Parquet
* XML

Databases support:
* Any SQL database supported by [SQLAlchemy](https://www.sqlalchemy.org/) 
* NoSQL databases: 
  * MongoDB

Metacrafter key features:
* 111 labeling rules
* all labels metadata collected into [Metacrafter registry](https://github.com/apicrafter/metacrafter-registry ) public repository
* 312 date detection rules/patterns, date detection using [qddate](https://github.com/ivbeg/qddate), "quick and dirty" date detection library
* extendable set of rules using PyParsing, exact text match and validation functions
* support any database supported by SQLAlchemy
* advanced context and language management. You could apply only rules relevant to certain data of choosen language
* built-in API server
* commercial support and additional rules available


## Command line examples

### File analysis examples

    # Scan CSV file
    $ metacrafter scan-file --format short somefile.csv

    # Scan CSV file with delimiter ';' and windows-1251 encoding
    $ metacrafter scan-file --format short --encoding windows-1251 --delimiter ';' somefile.csv

    # Scan JSON lines file, output results as stats table to file file
    $ metacrafter scan-file --format stats -o somefile_result.json somefile.jsonl


Result example of 'full' type of formatting
```    
key                   ftype    tags    matches
--------------------  -------  ------  -------------------------------
name                  str      uniq
addressresidence      str      uniq    address 59.80
addressactivities     str              address 50.98
addressobjects        str              address 28.00
bin                   int              ogrn 99.02
inn                   str              inn 100.00,inn 99.02
purposeaudit          str              runpa 8.82
dateregistration      str              datetime 94.12 (dt:date:date_2)
expirydate            str              datetime 18.63 (dt:date:date_2)
startdateactivity     str              datetime 28.43 (dt:date:date_2)
othergrounds          str      dict
startdateaudit        str              datetime 65.69 (dt:date:date_2)
workdays              int      dict
workhours             str      dict
formaudit             str      dict
namestatecontrol      str
assignment decree     str      dict
effectivedate         str      dict
Inspectionenddate     str      empty
riskcategory          str      dict
expirationdate        str      empty
startupnotifications  str      empty
daylastcheck          str      empty
otherreasonsrefusal   str      empty
numbersystem          str      empty

```


### Database analysis examples

    # Scan MongoDB database 'fns', save results as result.json and format output as 'stats'
    $ metacrafter scan-mongodb --dbname fns -o result.json -f full

    # Scan Postgres database 'dbname', with schema 'public'.
    $ metacrafter scan-db --schema public --connstr postgresql+psycopg2://username:password@127.0.0.1:15432/dbname



# Rules

All rules described as YAML files and by default rules loaded from directory 'rules' or from list of directories provided in .metacrafter file with YAML format

All rules could be applied to **fields** or **data** .

Compare engines defined in **match** parameter in rule description:
* text - scan text for exact match to one of text values. Text values delimited by comma (',')
* ppr - scan text for PyParsing. PyParsing rule defined as Python code with PyParsing objects like Word(nums, exact=4)
* func - scan text using Python function provided. Function shoud accept one string parameter and shoud return True or False

## How to write rules

### Function (func)

Example Russian administrative legal act/law matched by custom function
```
  runpabyfunc:
    key: runpa
    name: Russian legal act / law
    maxlen: 500
    minlen: 3
    priority: 1
    match: func
    type: data
    rule: metacrafter.rules.ru.gov.is_ru_law
```

### Exact text match (text)

Example midname matching by exact field name
```
  midname:
    key: person_midname
    name: Person midname by known
    rule: midname,secondname,middlename,mid_name,middle_name
    type: field
    match: text
```
### PyParsing rule (ppr)

Example Russian cadastral number
```
  rukadastr:
    key: rukadastr
    name: Russian land territory cadastral identifier
    rule: Word(nums, min=1, max=2) + Literal(':').suppress() + Word(nums, min=1, max=2) + Literal(':').suppress() + Word(nums, min=6, max=7) + Literal(':').suppress() + Word(nums, min=1, max=6)
    maxlen: 20
    minlen: 12
    priority: 1
    match: ppr
    type: data
```

## Detailed stats

Rule types:
- field based rules 75
- data based rules 36

Context:
- common 22
- companies 1
- crypto 3
- datetime 17
- geo 23
- government 1
- identifiers 3
- industry 1
- internet 11
- medical 1
- objectids 1
- persons 8
- pii 14
- science 2
- software 1
- values 1
- vehicles 1

Language:
- common 87
- en 18
- fr 1
- ru 5

Data/time patterns (qddate): 312


## Commercial support

Please write ibegtin@apicrafter.io or ivan@begtin.tech to request beta access to commercial API.
Commercial API support 195 fields and data rules and provided with dedicated support.

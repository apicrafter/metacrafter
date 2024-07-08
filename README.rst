Metacrafter
===========

Python command line tool and python engine to label table fields and
fields in data files. It could help to find meaningful data in your
tables and data files or to find Personal identifable information (PII).

Installation
------------

To install Python library use ``pip install metacrafter`` via pip or
``python setup.py install``

Features
--------

Metacrafter is a rule based tool that helps to label fields of the
tables in databases. It scans table and finds person names, surnames,
midnames, PII data, basic identifiers like UUID/GUID. These rules
written as .yaml files and could be easily extended.

File formats supported: \* CSV \* JSON lines \* JSON (array of records)
\* BSON \* Parquet \* XML

Metacrafter uses `iterable data <https://github.com/apicrafter/pyiterable>`__ python library 
that allows to process most data files with almost any compression Gzip, Bzip2, Snappy, Brotli and etc. 

Databases support: \* Any SQL database supported by
`SQLAlchemy <https://www.sqlalchemy.org/>`__ \* NoSQL databases: \*
MongoDB

Metacrafter key features: \* 111 labeling rules \* all labels metadata
collected into `Metacrafter
registry <https://github.com/apicrafter/metacrafter-registry>`__ public
repository \* 312 date detection rules/patterns, date detection using
`qddate <https://github.com/ivbeg/qddate>`__, “quick and dirty” date
detection library \* extendable set of rules using PyParsing, exact text
match and validation functions \* support any database supported by
SQLAlchemy \* advanced context and language management. You could apply
only rules relevant to certain data of choosen language \* built-in API
server \* commercial support and additional rules available

Command line examples
---------------------

File analysis examples
~~~~~~~~~~~~~~~~~~~~~~

::

   # Scan CSV file
   $ metacrafter scan file --format short somefile.csv

   # Scan CSV file with delimiter ';' and windows-1251 encoding
   $ metacrafter scan file --format short --encoding windows-1251 --delimiter ';' somefile.csv

   # Scan JSON lines file, output results as stats table to file file
   $ metacrafter scan file --format stats -o somefile_result.json somefile.jsonl

Result example of ‘full’ type of formatting

::

   key               ftype    tags    matches                                                                datatype_url
   ----------------  -------  ------  ---------------------------------------------------------------------  ----------------------------------------------------------
   Domain            str              fqdn 99.90                                                             https://registry.apicrafter.io/datatype/fqdn
   Primary domain    str              fqdn 100.00                                                            https://registry.apicrafter.io/datatype/fqdn
   Name              str              name 100.00                                                            https://registry.apicrafter.io/datatype/name
   Domain type       str      dict
   Organization      str
   Status            str      dict
   Region            str      dict    rusregion 22.95                                                        https://registry.apicrafter.io/datatype/rusregion
   GovSystem         str      dict
   HTTP Support      str      dict    boolean 100.00                                                         https://registry.apicrafter.io/datatype/boolean
   HTTPS Support     str      dict    boolean 100.00                                                         https://registry.apicrafter.io/datatype/boolean
   Statuscode        str      dict
   Is archived       str      empty
   Archives          str      empty
   Archive priority  str      dict
   Archive Strategy  str      dict
   ASN               str              asn 93.77                                                              https://registry.apicrafter.io/datatype/asn
   ASN Country code  str      dict    countrycode_alpha2 100.00,countrycode_alpha2 100.00,languagetag 99.56  https://registry.apicrafter.io/datatype/countrycode_alpha2
   IPs               str              ipv4 96.28                                                             https://registry.apicrafter.io/datatype/ipv4
   GovType           str      dict

Database analysis examples
~~~~~~~~~~~~~~~~~~~~~~~~~~

::

   # Scan MongoDB database 'fns', save results as result.json and format output as 'stats'
   $ metacrafter scan-mongodb --dbname fns -o result.json -f full

   # Scan Postgres database 'dbname', with schema 'public'.
   $ metacrafter scan-db --schema public --connstr postgresql+psycopg2://username:password@127.0.0.1:15432/dbname

Rules
=====

All rules described as YAML files and by default rules loaded from
directory ‘rules’ or from list of directories provided in .metacrafter
file with YAML format

All rules could be applied to **fields** or **data** .

Compare engines defined in **match** parameter in rule description: \*
text - scan text for exact match to one of text values. Text values
delimited by comma (‘,’) \* ppr - scan text for PyParsing. PyParsing
rule defined as Python code with PyParsing objects like Word(nums,
exact=4) \* func - scan text using Python function provided. Function
shoud accept one string parameter and shoud return True or False

How to write rules
------------------

Function (func)
~~~~~~~~~~~~~~~

Example Russian administrative legal act/law matched by custom function

::

     runpabyfunc:
       key: runpa
       name: Russian legal act / law
       maxlen: 500
       minlen: 3
       priority: 1
       match: func
       type: data
       rule: metacrafter.rules.ru.gov.is_ru_law

Exact text match (text)
~~~~~~~~~~~~~~~~~~~~~~~

Example midname matching by exact field name

::

     midname:
       key: person_midname
       name: Person midname by known
       rule: midname,secondname,middlename,mid_name,middle_name
       type: field
       match: text

PyParsing rule (ppr)
~~~~~~~~~~~~~~~~~~~~

Example Russian cadastral number

::

     rukadastr:
       key: rukadastr
       name: Russian land territory cadastral identifier
       rule: Word(nums, min=1, max=2) + Literal(':').suppress() + Word(nums, min=1, max=2) + Literal(':').suppress() + Word(nums, min=6, max=7) + Literal(':').suppress() + Word(nums, min=1, max=6)
       maxlen: 20
       minlen: 12
       priority: 1
       match: ppr
       type: data


Advanced rules
--------------

Metacrafter is looking for rules using .metacrafter file located in current or user home directory

Windows example of the _.metacrafter_ file
```yaml
rulepath:
   - C:\workspace\public\apicrafter\metacrafter-rules\rules\ 
```

You could write your own ruleset or to use already prepared rules from metacrafter-rules repository
For now you need to install rules code manually since there are some extensions and Python code to match certain rules
Please follow instructions in https://github.com/apicrafter/metacrafter-rules repository.


Commercial support
------------------

Please write ibegtin@apicrafter.io or ivan@begtin.tech to request beta
access to commercial API. Commercial API support 195 fields and data
rules and provided with dedicated support.

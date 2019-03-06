#!/usr/bin/env python3
'''
Read Kai's Schema TSV from stdin and output Frictionless Data Table Schema JSON template (http://frictionlessdata.io/specs/table-schema/).
'''

import sys
import json

header = sys.stdin.readline()
cols = [t.strip() for t in header.split(sep='\t')]

obj = {
    '@context': {
        'pm': 'http://purl.obolibrary.org/obo/PMO_00000000'
    },
    'profile': 'tabular-data-package',
    'name': '',
    'title': '',
    "path": "data.tsv",
    "dialect": {
        "delimiter": "\t",
        "header": true,
        "caseSensitiveHeader": true
    },
    "format": "csv",
    "mediatype": "text/tab-separated-values",
    "encoding": "UTF-8",
    'homepage': '',
    'licenses': [],
    'resources': [{
        'name': '',
        'title': '',
        'profile': 'tabular-data-resource',
        'schema': {
            'fields': [],
            'missingValues': [ '', 'nd' ] #TODO auto-detect missing values from data file
        }
    }]
}

for l in sys.stdin:
    inp = {}
    for t, f in zip(cols, l.split(sep='\t')):
        inp[t.strip()] = f.strip()

    field = {
        'name': inp['Parameter'],
        #'title': inp['Parameter'],
        'type': inp['type'] if inp['type'] else 'string',
        'format': inp['Frictionless Format'] if inp['Frictionless Format'] else 'default',
        #'description': inp['Comment'] + (', ' + inp['Method'] if inp['Method'] else ''),
        #'constraints': { 'required': True }, #TODO revisit this later
        'rdfType': inp['PURL/TEMP PURL'],
        'pm:unitRdf': inp['units PURL'],
        'pm:sourceCategory': inp['source category'],
        'pm:sourceURL': inp['source url']
    }
    obj['resources'][0]['schema']['fields'].append(field)

print(json.dumps(obj, indent=4))
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
        'pm': 'http://planetmicrobe.org/rdf/'
    },
    'profile': 'tabular-data-package',
    'name': '',
    'title': '',
    'homepage': '',
    'licenses': [],
    'resources': [{
        'name': '',
        'title': '',
        'profile': 'tabular-data-resource',
        'schema': {
            'fields': [],
            'missingValues': [ '' ]
        }
    }]
}

for l in sys.stdin:
    inp = {}
    for t, f in zip(cols, l.split(sep='\t')):
        inp[t.strip()] = f.strip()

    field = {
        'name': inp['Short Name'],
        'title': inp['Parameter'],
        'type': 'string' if not inp['Unit'] else 'number', # assume number if unit is specified
        'format': 'default',
        'description': inp['Comment'] + (', ' + inp['Method'] if inp['Method'] else ''),
        'constraints': { 'required': True },
        'rdfType': inp['PURL/TEMP PURL'],
        'pm:unitOfMeasure': inp['Unit']
    }
    obj['resources'][0]['schema']['fields'].append(field)

print(json.dumps(obj, indent=4))
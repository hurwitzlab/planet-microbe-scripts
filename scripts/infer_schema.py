#!/usr/bin/env python3
'''
Read TSV data file and output Frictionless Data Table Schema JSON template (http://frictionlessdata.io/specs/table-schema/).
'''

import sys
import json


from tableschema import Table

try:
    table = Table(sys.argv[1])
except Exception as e:
    print(e)

# print("Headers:", table.headers)
# print(table.read(keyed=True))
table.infer()
print(json.dumps(table.schema.descriptor, indent=4))

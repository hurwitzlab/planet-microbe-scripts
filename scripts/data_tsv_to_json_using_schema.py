#!/usr/bin/env python3
"""
Read TSV from stdin and output as JSON.
"""

import sys
import json
from pprint import pprint

skip_lines = int(sys.argv[1])
schema_file = sys.argv[2]

with open(schema_file) as f:
    schema = json.load(f)

while skip_lines > 0:
    sys.stdin.readline()
    skip_lines -= 1

cols = schema["properties"].keys()

print('[')

for l in sys.stdin:
    d = {}
    for t, f in zip(cols, l.split(sep="\t")):
        d[t.strip()] = f.strip()
    print(json.dumps(d, indent=4), ',')

print(']')

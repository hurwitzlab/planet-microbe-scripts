#!/usr/bin/env python3
"""
Read TSV from stdin and output as JSON.
"""

import sys
import json

skip_lines = int(sys.argv[1])


while skip_lines > 0:
    sys.stdin.readline()
    skip_lines -= 1

header = sys.stdin.readline()
cols = [t.strip() for t in header.split(sep="\t")]

print('[')

for l in sys.stdin:
    d = {}
    for t, f in zip(cols, l.split(sep="\t")):
        d[t.strip()] = f.strip()
    print(json.dumps(d, indent=4), ',')

print(']')

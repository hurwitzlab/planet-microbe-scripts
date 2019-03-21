#!/usr/bin/env python3
"""
Read Kai"s Schema TSV from stdin and output Frictionless Data Table Schema JSON template (http://frictionlessdata.io/specs/table-schema/).
"""

import sys
import json

header = sys.stdin.readline()
cols = [t.strip() for t in header.split(sep="\t")]

obj = {
    "@context": {
        "pm": "http://purl.obolibrary.org/obo/PMO_00000000"
    },
    "profile": "tabular-data-package",
    "name": "",
    "title": "",
    "pm:projectType": "marine metagenome",
    "homepage": "",
    "keywords": [],
    "sources": [],
    "licenses": [],
    "resources": [{
        "name": "",
        "title": "",
        "description": "",
        "profile": "tabular-data-resource",
        "path": "data.tsv",
        "dialect": {
            "delimiter": "\t",
            "header": True,
            "caseSensitiveHeader": True
        },
        "format": "csv",
        "mediatype": "text/tab-separated-values",
        "encoding": "UTF-8",
        "hash": "",
        "schema": {
            "fields": [],
            "missingValues": [ "", "nd" ] #TODO auto-detect missing values from data file
        }
    }]
}

for l in sys.stdin:
    inp = {}
    for t, f in zip(cols, l.split(sep="\t")):
        inp[t.strip()] = f.strip()

    field = {
        "name": inp["parameter"],
        "type": inp["frictionless type"] if inp["frictionless type"] else "string",
        "format": inp["frictionless format"] if inp["frictionless format"] else "default",
        #"constraints": { "required": True }, #TODO revisit this later
        "rdfType": inp["rdf type purl"],
        "pm:unitRdfType": inp["units purl"],
        #"pm:sourceCategory": inp["pm:source category"],
        "pm:sourceUrl": inp["pm:source url"],
        "pm:measurementSourceRdfType": inp["measurement source purl"],
        "pm:measurementSourceProtocolUrl": inp["pm:measurement source protocol"],
        "pm:searchable": True if inp["pm:searchable"].lower() == "true" else False
    }
    obj["resources"][0]["schema"]["fields"].append(field)

print(json.dumps(obj, indent=2))
# Planet Microbe Schemas

This repository contains scripts and example mapping tables for generating Frictionless 
Data tabular data package schemas.

For example, this command generates a tabular data package JSON template for the OSD data set: 
```
cat example_ontology_mappings/OSD.tsv | ./scripts/schema_tsv_to_json.py > example_data_packages/osd/datapackage.json
```

The JSON was then hand-edited to add missing information and correct names, types, and units.

Units were standardized to the FD 'units-and-prefies' example data package (https://github.com/frictionlessdata/example-data-packages/tree/master/units-and-prefixes).  Need to revisit this at some point and use a better standard instead.

For more information on FD Table Schemas see http://frictionlessdata.io/specs/table-schema/ 

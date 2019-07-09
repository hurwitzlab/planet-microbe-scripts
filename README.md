# Planet Microbe Scripts

This repository contains scripts and example mapping tables for generating Frictionless 
Data tabular data package schemas and loading data packages into the Postgres database.

## Creating Data Package Templates

This command generates a tabular data package JSON template for the OSD data set: 
```
cat example_ontology_mappings/OSD.tsv | ./scripts/schema_tsv_to_json.py > example_data_packages/osd/datapackage.json
```

The JSON was then hand-edited to add missing information and correct names, types, and units.

Units were standardized to the FD 'units-and-prefies' example data package (https://github.com/frictionlessdata/example-data-packages/tree/master/units-and-prefixes).  Need to revisit this at some point and use a better standard instead.

For more information on FD Table Schemas see http://frictionlessdata.io/specs/table-schema/ 

## Loading Data Packages

Make sure you have a database and schema:
```
createdb planetmicrobe -U planetmicrobe
psql -d planetmicrobe -U postgres -c "CREATE EXTENSION postgis;"
psql -d planetmicrobe -U planetmicrobe -f scripts/postgres.sql
```

Install Data Packages:
```
git clone git@github.com:hurwitzlab/planet-microbe-datapackages.git ..
```

Create Python virtual environment and run load script:
```
virtualenv -p /usr/bin/python3.6 python3
source python3/bin/activate
pip install simplejson datapackage psycopg2 shapely pint biopython
scripts/load_datapackage_postgres2.py -d planetmicrobe -u planetmicrobe -p <password>  ../planet-microbe-datapackages/HOT-Chisholm/datapackage.json
```

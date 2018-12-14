#!/usr/bin/env python3
"""
Load Data Package schema and data into Postgres JSON tables
"""

import sys
import json
from datapackage import Package, Resource
from tableschema import Table
import psycopg2


if __name__ == "__main__":
    conn = psycopg2.connect("host='localhost' dbname='jsontest' user='mbomhoff' password=''")
    cursor = conn.cursor()

    package = Package(sys.argv[1])
    print('Name: ', package.descriptor['name'])
    if not package.valid:
        print(package.errors)
    print('Resources: ', package.resource_names)

    for rname in package.resource_names:
        schema_name = package.descriptor['name'] + '-' + rname
        resource = package.get_resource(rname)
        print('Loading schema ...')
        schema = resource.schema
        cursor.execute('INSERT INTO schema (name,fields) VALUES (%s,%s) RETURNING schema_id;', [schema_name, json.dumps(schema.descriptor)])
        schema_id = cursor.fetchone()[0]
        conn.commit()
        print("schema_id:", schema_id)

        count = 0
        for row in resource.read(keyed=True):
            arrVals = []
            for key in row:
                type = schema.get_field(key).type
                if type == 'number':
                    if row[key] == None:
                        arrVals.append((None, None))
                    else:
                        arrVals.append((float(row[key]), None))
                else: #if type == 'string' or type == 'datetime':
                    arrVals.append((None, str(row[key])))
            stmt = cursor.mogrify("INSERT INTO sample (schema_id,fields) VALUES(%s,%s::field_type[])", [schema_id,arrVals])
            cursor.execute(stmt)
            conn.commit()
            count += 1
            print('\rLoading data ...', count, end='')
        print()

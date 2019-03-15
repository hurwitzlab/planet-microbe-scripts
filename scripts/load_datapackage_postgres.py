#!/usr/bin/env python3
"""
Load Data Package schema and data into Postgres
"""

import sys
import simplejson as json
import decimal
from datetime import date, datetime
from datapackage import Package, Resource
from tableschema import Table
import psycopg2


DB_NAME = sys.argv[1]
DB_USERNAME = 'mbomhoff'
DP_FILE_PATH = sys.argv[2]


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return (str(obj) for obj in [obj])
    raise TypeError("Type %s not serializable" % type(obj))


# def db_create_schema(fdSchema):
#     #TODO check if schema already exists
#     schemaId = cursor.execute('INSERT INTO schema (name) VALUES (%s) RETURNING schema_id;', [schema_name])
#
#     for f in fdSchema.fields:
#         cursor.execute('INSERT INTO field ...


if __name__ == "__main__":
    conn = psycopg2.connect("host='' dbname='" + DB_NAME + "' user='" + DB_USERNAME + "' password=''")
    cursor = conn.cursor()

    package = Package(DP_FILE_PATH)
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
        try:
            for row in resource.read():
                # Array Implementation
                latitude = None
                longitude = None
                numberVals = []
                stringVals = []
                datetimeVals = []
                i = 0
                for val in row:
                    type = schema.fields[i].type
                    rdfType = schema.descriptor['fields'][i]['rdfType']
                    if type == 'number':
                        if val == None: # no data
                            numberVals.append(None)
                        else:
                            numberVals.append(float(val))
                            if rdfType == "http://purl.obolibrary.org/obo/OBI_0001620":
                                latitude = float(val)
                            elif rdfType == "http://purl.obolibrary.org/obo/OBI_0001621":
                                longitude = float(val)
                        stringVals.append(None)
                        datetimeVals.append(None)
                    elif type == 'string':
                        stringVals.append(str(val))
                        numberVals.append(None)
                        datetimeVals.append(None)
                    elif type == 'datetime' or type == 'date': #TODO handle time zone
                        stringVals.append(None)
                        numberVals.append(None)
                        datetimeVals.append(val)
                    else:
                        print("Unknown type:", type)
                        exit(-1)
                    i += 1

                stmt = cursor.mogrify(
                    "INSERT INTO sample (schema_id,locations,number_vals,string_vals,datetime_vals) VALUES(%s,ST_SetSRID(ST_MakeLine(ARRAY[ST_MakePoint(%s,%s)]),4326),%s,%s,%s::timestamp[])",
                    [schema_id,longitude,latitude,numberVals,stringVals,datetimeVals]
                )
                cursor.execute(stmt)
                conn.commit()
                count += 1
                print('\rLoading data ...', count, end='')
            print()
        except Exception as e:
            print(e)
            if e.errors:
                print(*e.errors, sep='\n')

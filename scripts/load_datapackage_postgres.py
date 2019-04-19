#!/usr/bin/env python3
"""
Load Data Package schema and data into Postgres
"""

import sys
import argparse
import simplejson as json
from datapackage import Package, Resource
from tableschema import Table
import psycopg2
from shapely.geometry import MultiPoint
from shapely import wkb


# def db_create_schema(fdSchema):
#     #TODO check if schema already exists
#     schemaId = cursor.execute('INSERT INTO schema (name) VALUES (%s) RETURNING schema_id;', [schema_name])
#
#     for f in fdSchema.fields:
#         cursor.execute('INSERT INTO field ...


LATITUDE_PURLS = [
    "http://purl.obolibrary.org/obo/OBI_0001620",
    "http://purl.obolibrary.org/obo/PMO_00000076",
    "http://purl.obolibrary.org/obo/PMO_00000079"
]

LONGITUDE_PURLS = [
    "http://purl.obolibrary.org/obo/OBI_0001621",
    "http://purl.obolibrary.org/obo/PMO_00000077",
    "http://purl.obolibrary.org/obo/PMO_00000078"
]


def main(args=None):
    conn = psycopg2.connect("host='' dbname='" + args['dbname'] + "' user='" + args['username'] + "' password=''")
    cursor = conn.cursor()

    package = Package(args['filepath'])
    print('Name: ', package.descriptor['name'])
    if not package.valid:
        print(package.errors)
    print('Resources: ', package.resource_names)

    for rname in package.resource_names:
        if 'resource' in args and args['resource'] != None and rname != args['resource']:
            continue

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
                latitudeVals = []
                longitudeVals = []
                numberVals = []
                stringVals = []
                datetimeVals = []
                i = 0
                for val in row:
                    type = schema.fields[i].type
                    rdfType = schema.descriptor['fields'][i]['rdfType']
                    searchable = allFields[i]['pm:searchable']
                    if type == 'number':
                        if val == None: # no data
                            numberVals.append(None)
                        else:
                            numberVals.append(float(val))
                            try:
                                if LATITUDE_PURLS.index(rdfType) >= 0 and searchable:
                                    latitudeVals.append(float(val))
                            except ValueError:
                                pass
                            try:
                                if LONGITUDE_PURLS.index(rdfType) >= 0 and searchable:
                                    longitudeVals.append(float(val))
                            except ValueError:
                                pass
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

                locations = MultiPoint(list(zip(longitudeVals, latitudeVals)))
                stmt = cursor.mogrify(
                    "INSERT INTO sample (schema_id,locations,number_vals,string_vals,datetime_vals) VALUES(%s,ST_SetSRID(%s::geography, 4326),%s,%s,%s::timestamp[])",
                    [schema_id,locations.wkb_hex,numberVals,stringVals,datetimeVals]
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load datapackage into database.')
    parser.add_argument('-d', '--dbname')
    parser.add_argument('-u', '--username')
    parser.add_argument('-r', '--resource')
    parser.add_argument('filepath')

    print(vars(parser.parse_args()))

    main(args={k: v for k, v in vars(parser.parse_args()).items() if v})

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


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return (str(obj) for obj in [obj])
    raise TypeError("Type %s not serializable" % type(obj))


if __name__ == "__main__":
    conn = psycopg2.connect("host='' dbname='arraytestgis' user='mbomhoff' password=''")
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
                    elif type == 'datetime': #TODO handle time zone
                        stringVals.append(None)
                        numberVals.append(None)
                        datetimeVals.append(str(val))
                    else:
                        print("Unknown type:", type)
                        exit(-1)
                    i += 1

                stmt = cursor.mogrify("INSERT INTO sample (schema_id,location,number_vals,string_vals,datetime_vals) VALUES(%s,ST_SetSRID(ST_MakePoint(%s,%s),4326),%s,%s,%s::timestamp[])",
                                      [schema_id,longitude,latitude,numberVals,stringVals,datetimeVals])
                cursor.execute(stmt)

                ## JSON implementation
                #cursor.execute('INSERT INTO sample (schema_id,fields) VALUES (%s,%s);', [schema_id, json.dumps(row, default=json_serial, iterable_as_array=True)])

                ## Composite array implementation
                # arrVals = []
                # for key in row:
                #     type = schema.get_field(key).type
                #     if type == 'number':
                #         if row[key] == None:
                #             arrVals.append((None, None))
                #         else:
                #             arrVals.append((float(row[key]), None))
                #     else: #if type == 'string' or type == 'datetime':
                #         arrVals.append((None, str(row[key])))
                #stmt = cursor.mogrify("INSERT INTO sample (schema_id,fields) VALUES(%s,%s::field_type[])", [schema_id,arrVals]) # for composite array implementation
                #cursor.execute(stmt)

                ## Relational implementation
                # cursor.execute("INSERT INTO sample (schema_id) VALUES(%s) RETURNING sample_id;", [schema_id])
                # sample_id = cursor.fetchone()[0]
                # fieldNum = 0
                # for key in row:
                #     type = schema.get_field(key).type
                #     if type == 'number':
                #         if row[key] == None:
                #             cursor.execute('INSERT INTO field (schema_id,sample_id,field_num,string_value,number_value) VALUES (%s,%s,%s,%s,%s);', [schema_id,sample_id,fieldNum,None,None])
                #         else:
                #             cursor.execute('INSERT INTO field (schema_id,sample_id,field_num,string_value,number_value) VALUES (%s,%s,%s,%s,%s);', [schema_id,sample_id,fieldNum,None,float(row[key])])
                #     else:
                #         cursor.execute('INSERT INTO field (schema_id,sample_id,field_num,string_value,number_value) VALUES (%s,%s,%s,%s,%s);', [schema_id,sample_id,fieldNum,str(row[key]),None])
                #     fieldNum += 1

                conn.commit()
                count += 1
                print('\rLoading data ...', count, end='')
            print()
        except Exception as e:
            print(e)
            if e.errors:
                print(*e.errors, sep='\n')

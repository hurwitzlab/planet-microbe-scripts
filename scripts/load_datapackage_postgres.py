#!/usr/bin/env python3
"""
Load Data Package schema and data into Postgres JSON tables
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
    conn = psycopg2.connect("host='' dbname='arraytest' user='mbomhoff' password=''")
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
        for row in resource.read():
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

            ## Array Implementation
            numberVals = []
            stringVals = []
            i = 0
            for val in row:
                type = schema.fields[i].type
                if type == 'number':
                    if val == None:
                        numberVals.append(None)
                    else:
                        numberVals.append(float(val))
                    stringVals.append(None)
                else: #if type == 'string' or type == 'datetime':
                    stringVals.append(str(val))
                    numberVals.append(None);
                i += 1
            stmt = cursor.mogrify("INSERT INTO sample (schema_id,number_vals,string_vals) VALUES(%s,%s,%s)", [schema_id,numberVals,stringVals])
            cursor.execute(stmt)

            ## JSON implementation
            #cursor.execute('INSERT INTO sample (schema_id,fields) VALUES (%s,%s);', [schema_id, json.dumps(row, default=json_serial, iterable_as_array=True)])

            # Relational implementation
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

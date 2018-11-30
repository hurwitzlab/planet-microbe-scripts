#!/usr/bin/env python3
"""
Load Data Package schema and data into MongoDB
"""

import sys
import simplejson as json
import decimal
from datetime import date, datetime
from datapackage import Package, Resource
from tableschema import Table
from pymongo import MongoClient
from bson.decimal128 import Decimal128


# def json_serial(obj):
#     """JSON serializer for objects not serializable by default json code"""
#
#     if isinstance(obj, (datetime, date)):
#         return obj.isoformat()
#     if isinstance(obj, decimal.Decimal):
#         return (str(obj) for obj in [obj])
#     raise TypeError("Type %s not serializable" % type(obj))


def to_mongo(doc):
    """Convert data package row to mongo doc"""
    for key in doc:
        if isinstance(doc[key], decimal.Decimal):
            doc[key] = float(doc[key])
    return doc


if __name__ == "__main__":
    client = MongoClient('localhost', 27017)
    db = client['pm_test2']
    schemas = db['schemas']
    samples = db['samples']

    package = Package(sys.argv[1])
    print('Name: ', package.descriptor['name'])
    if not package.valid:
        print(package.errors)
    print('Resources: ', package.resource_names)

    for rname in package.resource_names:
        try:
            schema_name = package.descriptor['name'] + '-' + rname
            resource = package.get_resource(rname)
            print('Loading schema ...')
            schema = resource.schema.descriptor
            schema['name'] = schema_name
            schema_id = schemas.insert(schema)

            #print(json.dumps(resource, default=json_serial, iterable_as_array=True, indent=2))
            count = 0
            for row in resource.read(keyed=True):
                row['__schema_name'] = schema_name
                row['__schema_id'] = schema_id
                row2 = to_mongo(row)
                id = samples.insert(row2)
                count += 1
                print('\rLoading data ...', count, end='')
            print()

        except Exception as e:
            print(e)
            if e.errors:
                print(*e.errors, sep='\n')

#!/usr/bin/env python3
"""
Validate Data Package data files against schema
"""

import sys
import simplejson as json
import decimal
from datetime import date, datetime
from datapackage import Package, Resource
from tableschema import Table


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return (str(obj) for obj in [obj])
    raise TypeError("Type %s not serializable" % type(obj))


if __name__ == "__main__":
    package = Package(sys.argv[1])
    print('Name: ', package.descriptor['name'])
    if not package.valid:
        print(package.errors)
    print('Resources: ', package.resource_names)

    for rname in package.resource_names:
        schema_name = package.descriptor['name'] + '-' + rname
        resource = package.get_resource(rname)
        print('Validating schema ...')
        schema = resource.schema

        count = 0
        try:
            for row in resource.read():
                print(row)

    #             for val in row:
    #                 type = schema.fields[i].type
    #                 rdfType = schema.descriptor['fields'][i]['rdfType']
    #                 if type == 'number':
    #                     if val == None: # no data
    #                         numberVals.append(None)
    #                     else:
    #                         numberVals.append(float(val))
    #                         if rdfType == "http://purl.obolibrary.org/obo/OBI_0001620":
    #                             latitude = float(val)
    #                         elif rdfType == "http://purl.obolibrary.org/obo/OBI_0001621":
    #                             longitude = float(val)
    #                     stringVals.append(None)
    #                     datetimeVals.append(None)
    #                 elif type == 'string':
    #                     stringVals.append(str(val))
    #                     numberVals.append(None)
    #                     datetimeVals.append(None)
    #                 elif type == 'datetime': #TODO handle time zone
    #                     stringVals.append(None)
    #                     numberVals.append(None)
    #                     datetimeVals.append(str(val))
    #                 else:
    #                     print("Unknown type:", type)
    #                     exit(-1)
    #                 i += 1
    #
    #             count += 1
        except Exception as e:
            print(e)
            if e.errors:
                print(*e.errors, sep='\n')

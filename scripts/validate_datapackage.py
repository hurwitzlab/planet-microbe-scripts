#!/usr/bin/env python3
"""
Validate Data Package data files against schema
"""

import sys
import simplejson as json
import decimal
import argparse
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


def main(args=None):
    package = Package(args['filepath'][0])
    #print('Name: ', package.descriptor['name'])
    if not package.valid:
        print(package.errors)
    #print('Resources: ', package.resource_names)

    for rname in package.resource_names:
        if 'resource' in args and rname != args['resource']:
            continue

        resource = package.get_resource(rname)
        #print('Validating resource', rname)
        schema = resource.schema

        totalCount = 0
        try:
            for row in resource.read():
                totalCount += 1
        except Exception as e:
            print('Resource %s:' % rname)
            print(e)
            if e.errors:
                print(*e.errors, sep='\n')

        #print("Total rows:", totalCount)
        #print("Resource", rname, "is valid")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load datapackage into database.')
    parser.add_argument('-r', '--resource')
    parser.add_argument('filepath', nargs=1)

    main(args={k: v for k, v in vars(parser.parse_args()).items() if v})

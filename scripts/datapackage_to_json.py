#!/usr/bin/env python3
"""
Read Data Package JSON from stdin and output data as JSON.
"""

import sys
import simplejson as json
import decimal
from datetime import date, datetime
from datapackage import Package
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

    if not package.valid:
        print(package.errors)

    print("Resources: ", package.resource_names)

    try:
        for rname in package.resource_names:
            resource = package.get_resource(rname).read(keyed=True)
            print(json.dumps(resource, default=json_serial, iterable_as_array=True, indent=2))
    except Exception as e:
        print(e)
        if e.errors:
            print(*e.errors, sep="\n")

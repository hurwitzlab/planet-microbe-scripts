#!/usr/bin/env python3
"""
Validate Data Package data files against schema
"""

import argparse
from datapackage import Package


def main(args=None):
    package = Package(args['filepath'][0])
    #print('Name: ', package.descriptor['name'])
    if not package.valid:
        print(package.errors)
    #print('Resources: ', package.resource_names)

    if not 'pm:projectType' in package.descriptor:
        print('PM error: missing pm:projectType in package')

    for rname in package.resource_names:
        if 'resource' in args and rname != args['resource']:
            continue

        resource = package.get_resource(rname)
        #print('Validating resource', rname)

        if not 'pm:resourceType' in resource.descriptor:
            print('PM error: missing pm:resourceType in resource', rname)

        for field in resource.descriptor['schema']['fields']:
            for property in ['pm:unitRdfType', 'pm:sourceUrl', 'pm:measurementSourceRdfType', 'pm:measurementSourceProtocolUrl']:
                if not property in field:
                    print('PM error: missing', property, 'in resource', rname)

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
    parser = argparse.ArgumentParser(description='Validate data package and show errors')
    parser.add_argument('-r', '--resource', help='optional resource name to restrict validation to, otherwise validate entire package')
    parser.add_argument('filepath', nargs=1, help='path to datapackage.json file')

    main(args={k: v for k, v in vars(parser.parse_args()).items() if v})

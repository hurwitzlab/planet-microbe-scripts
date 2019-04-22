#!/usr/bin/env python3
"""
Load Data Package schema and data into Postgres database
"""

import sys
import argparse
import simplejson as json
from datapackage import Package, Resource
from tableschema import Table
import psycopg2
from shapely.geometry import MultiPoint
from shapely import wkb


CAMPAIGN_CRUISE_DB_SCHEMA = {
    "name": "http://purl.obolibrary.org/obo/PMO_00000060",
    "description": "",
    "deployment": "http://purl.obolibrary.org/obo/PMO_00000007",
    "start_location": "http://purl.obolibrary.org/obo/pmo.owl/PMO_00000144",
    "end_location": "http://purl.obolibrary.org/obo/pmo.owl/PMO_00000145",
    "start_time": "http://purl.obolibrary.org/obo/pmo.owl/PMO_00000137",
    "end_time": "http://purl.obolibrary.org/obo/pmo.owl/PMO_00000138",
    "urls": [
        "http://purl.obolibrary.org/obo/pmo.owl/PMO_00000133",
        "http://purl.obolibrary.org/obo/PMO_00000047"
    ]
}

SAMPLING_EVENT_DB_SCHEMA = {
    "sampling_event_id": "http://purl.obolibrary.org/obo/PMO_00000056",
    "sampling_event_type": "http://purl.obolibrary.org/obo/pmo.owl/PMO_00000146",
    "campaign_id": "http://purl.obolibrary.org/obo/PMO_00000060",
    "latitude": "http://purl.obolibrary.org/obo/OBI_0001621",
    "longitude": "http://purl.obolibrary.org/obo/OBI_0001620",
    "start_time": "http://purl.obolibrary.org/obo/PMO_00000008",
    "station": "http://purl.obolibrary.org/obo/pmo.owl/PMO_00000149"
}

SAMPLE_EVENT_ID_PURL = "http://purl.obolibrary.org/obo/PMO_00000056"

SAMPLE_ID_PURL = "http://purl.obolibrary.org/obo/OBI_0001901"

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


def get_resources_by_type(type, resources):
    return list(filter(lambda r: r.descriptor['pm:resourceType'] == type, resources))


def get_fields_by_type(type, fields):
    return list(filter(lambda f: f.descriptor['rdfType'] == type, fields))


def get_fields_by_types(types, fields):
    return list(filter(lambda f: f.descriptor['rdfType'] and any(f.descriptor['rdfType'] in t for t in types), fields))


def load_resource(db, resource, dbSchema, tableName, insertMethod):
    fieldMap = {}
    for k,v in dbSchema.items():
        if not k or not v:
            continue

        if isinstance(v, list):
            fields = get_fields_by_types(v, resource.schema.fields)
        else:
            fields = get_fields_by_type(v, resource.schema.fields)

        if not fields:
            raise Exception("Missing field ", v)
        elif len(fields) > 1 and not isinstance(fields, list):
            raise Exception("Too many values found for ", v)

        fieldMap[k] = fields

    results = {}
    try:
        for row in resource.read(keyed=True):
            #print(row)
            obj = {}
            for k,fields in fieldMap.items():
                vals = list(filter(lambda v: v != None, map(lambda f: row[f.name], fields)))
                obj[k] = vals
            id = insertMethod(db, tableName, obj)
            results[id] = obj
    except Exception as e:
        print(e)
        if e.errors:
            print(*e.errors, sep='\n')

    return results


def load_campaigns(db, package):
    campaigns = get_resources_by_type("campaign", package.resources)
    if not campaigns:
        raise Exception("No campaign resource found")
    elif len(campaigns) > 1:
        raise Exception("More than one campaign resource found")

    load_resource(db, campaigns[0], CAMPAIGN_CRUISE_DB_SCHEMA, "campaign", insert_campaign)
    db.commit()


def insert_campaign(db, tableName, obj):
    cursor = db.cursor()
    cursor.execute(
        'INSERT INTO campaign (campaign_type,name,deployment,start_location,end_location,start_time,end_time,urls) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING campaign_id',
        ['cruise', obj['name'][0], obj['deployment'][0], obj['start_location'][0], obj['end_location'][0], obj['start_time'][0], obj['end_time'][0], obj['urls']]
    )
    return cursor.fetchone()[0]


def load_sampling_events(db, package, campaigns):
    events = get_resources_by_type("sampling_event", package.resources)
    if not events:
        raise Exception("No sampling_event resource found")
    elif len(events) > 1:
        raise Exception("More than one sampling_event resource found")

    entries = load_resource(db, events[0], SAMPLING_EVENT_DB_SCHEMA, "sampling_event", insert_sampling_event)
    db.commit()
    return entries


def insert_sampling_event(db, tableName, obj):
    cursor = db.cursor()
    cursor.execute('SELECT campaign_id FROM campaign WHERE name=%s', obj['campaign_id']) # FIXME campaign name may not be unique
    campaign_id = cursor.fetchone()[0]
    cursor.execute(
        'INSERT INTO sampling_event (sampling_event_type,campaign_id,locations,start_time,data_url) VALUES (%s,%s,ST_SetSRID(ST_MakeLine(ARRAY[ST_MakePoint(%s,%s)]),4326),%s,%s) RETURNING sampling_event_id',
        [obj['sampling_event_type'][0], campaign_id, obj['longitude'][0], obj['latitude'][0], obj['start_time'][0], "FIXME"]
    )
    return cursor.fetchone()[0]


def load_samples(db, package, sampling_events):
    resources = get_resources_by_type("sample", package.resources)
    if not resources:
        raise Exception("No sample resources found")

    # Join schema and data for all sample resources
    allFields = []
    valuesBySampleId = {}
    sampleIdToSampleEventId = {}
    for i in range(len(resources)):
        resource = resources[i]
        print("Sample resource", resource.name)

        # Determine position of Sample Event ID and Sample ID fields
        fields = resource.schema.descriptor['fields']
        sampleIdPos = list(map(lambda f: f['rdfType'], fields)).index(SAMPLE_ID_PURL)
        sampleEventIdPos = list(map(lambda f: f['rdfType'], fields)).index(SAMPLE_EVENT_ID_PURL)

        if i > 0:
            del fields[sampleIdPos]
            del fields[sampleEventIdPos]
        allFields.extend(fields)

        for row in resource.read():
            sampleId = row[sampleIdPos]
            sampleEventId = row[sampleEventIdPos]
            sampleIdToSampleEventId[sampleId] = sampleEventId

            if i > 0:
                del row[sampleIdPos]
                del row[sampleEventIdPos]

            if not sampleId in valuesBySampleId:
                valuesBySampleId[sampleId] = []
            valuesBySampleId[sampleId].extend(row)

    # Load schema
    schema_id = insert_schema(db, package.descriptor['name'], { "fields": allFields })

    # Load sample values
    cursor = db.cursor()
    samples = {}
    count = 0
    for id in valuesBySampleId:
        latitudeVals = []
        longitudeVals = []
        numberVals = []
        stringVals = []
        datetimeVals = []
        i = 0
        for val in valuesBySampleId[id]:
            type = allFields[i]['type']
            rdfType = allFields[i]['rdfType']
            searchable = allFields[i]['pm:searchable']
            if type == 'number':
                if val == None:  # no data
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
            elif type == 'datetime' or type == 'date':  # TODO handle time zone
                stringVals.append(None)
                numberVals.append(None)
                datetimeVals.append(val)
            else:
                print("Unknown type:", type)
                exit(-1)
            i += 1

        sampling_event_id = None
        for eventId in sampling_events:
            if sampling_events[eventId]['sampling_event_id'][0] == sampleIdToSampleEventId[id]:
                sampling_event_id = eventId
                break

        locations = MultiPoint(list(zip(longitudeVals, latitudeVals)))

        stmt = cursor.mogrify(
            "INSERT INTO sample (schema_id,sampling_event_id,locations,number_vals,string_vals,datetime_vals) VALUES(%s,%s,ST_SetSRID(%s::geography, 4326),%s,%s,%s::timestamp[]) RETURNING sample_id",
            [schema_id, sampling_event_id, locations.wkb_hex, numberVals, stringVals, datetimeVals]
        )
        cursor.execute(stmt)
        sample_id = cursor.fetchone()[0]
        samples[sample_id] = valuesBySampleId[id]
        db.commit()
        count += 1
        print('\rLoading samples', count, end='')
    print()

    return samples


def insert_schema(db, name, schema):
    cursor = db.cursor()
    print('Loading schema "%s"' % name)
    cursor.execute('INSERT INTO schema (name,fields) VALUES (%s,%s) RETURNING schema_id',
                   [name, json.dumps(schema)])
    schema_id = cursor.fetchone()[0]
    db.commit()
    print("Added schema", schema_id)
    return schema_id


def insert_project(db, package, samples):
    cursor = db.cursor()

    # Create project_type entry
    type = package.descriptor['pm:projectType']

    if not type:
        raise Exception("Missing pm:projectType field")

    cursor.execute('SELECT project_type_id FROM project_type WHERE name=%s', [type])
    if cursor.rowcount == 0:
        cursor.execute('INSERT INTO project_type (name) VALUES (%s) RETURNING project_type_id', [type])
    project_type_id = cursor.fetchone()[0]

    # Create project entry
    title = package.descriptor['title']
    description = package.descriptor['description']
    homepage = package.descriptor['homepage']

    cursor.execute(
        'INSERT INTO project (project_type_id,accn,name,description,url) VALUES (%s,%s,%s,%s,%s) RETURNING project_id',
        [project_type_id, "foo", title, description, homepage]
    )
    project_id = cursor.fetchone()[0]
    print("Added project", project_id)

    # Create project_to_sample entries
    for sampleId in samples:
        cursor.execute('INSERT INTO project_to_sample (project_id,sample_id) VALUES (%s,%s)', [project_id, sampleId])

    db.commit()


def main(args=None):
    conn = psycopg2.connect(host='', dbname=args['dbname'], user=args['username'], password='')
    cursor = conn.cursor()

    package = Package(args['filepath'])
    print('Name: ', package.descriptor['name'])
    if not package.valid:
        print(package.errors)

    campaigns = load_campaigns(conn, package)
    sampling_events = load_sampling_events(conn, package, campaigns)
    samples = load_samples(conn, package, sampling_events)
    insert_project(conn, package, samples)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load datapackage into database.')
    parser.add_argument('-d', '--dbname')
    parser.add_argument('-u', '--username')
    parser.add_argument('-r', '--resource')
    parser.add_argument('filepath')

    main(args={k: v for k, v in vars(parser.parse_args()).items() if v})

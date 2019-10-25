#!/usr/bin/env python3
"""
Load Data Package schema and data into Postgres database

load_datapackage_postgres2.py -d <database> -u <username> -p <password> datapackage.json
"""

import sys
import os
import argparse
import subprocess
import simplejson as json
from datapackage import Package, Resource
from tableschema import Table
import psycopg2
from shapely.geometry import MultiPoint
from shapely import wkb
# from pint import UnitRegistry


# ureg = UnitRegistry()
# Q_ = ureg.Quantity


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
    "latitude": "http://purl.obolibrary.org/obo/OBI_0001620",
    "longitude": "http://purl.obolibrary.org/obo/OBI_0001621",
    "start_latitude": "http://purl.obolibrary.org/obo/PMO_00000076",
    "start_longitude": "http://purl.obolibrary.org/obo/PMO_00000077",
    "end_latitude": "http://purl.obolibrary.org/obo/PMO_00000079",
    "end_longitude": "http://purl.obolibrary.org/obo/PMO_00000078",
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

        # if not fields:
        #     raise Exception("Missing field ", v)
        # elif len(fields) > 1 and not isinstance(fields, list):
        #     raise Exception("Too many values found for ", v)

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
    resources = get_resources_by_type("campaign", package.resources)
    if not resources:
        print("No campaign resource found") #raise Exception("No campaign resource found")
        return

    allCampaigns = []
    for i in range(len(resources)):
        resource = resources[i]
        print("Campaign resource:", resource.name)
        campaigns = load_resource(db, resource, CAMPAIGN_CRUISE_DB_SCHEMA, "campaign", insert_campaign)
        allCampaigns.append(campaigns)
        db.commit()

    return allCampaigns


def insert_campaign(db, tableName, obj):
    #print("insert_campaign:", obj)
    if not obj['name']:
        raise Exception("Missing campaign name")
    if not obj['deployment']:
        raise Exception("Missing campaign deployment")
    if not obj['start_location']:
        raise Exception("Missing campaign start_location")
    if not obj['end_location']:
        raise Exception("Missing campaign end_location")
    if not obj['start_time']:
        raise Exception("Missing campaign start_time")
    if not obj['end_time']:
        raise Exception("Missing campaign end_time")
    if not obj['urls'] or len(obj['urls']) == 0:
        raise Exception("Missing campaign urls")

    cursor = db.cursor()

    cursor.execute('SELECT campaign_id FROM campaign WHERE name=%s', obj['name'])
    if cursor.rowcount > 0:
        return cursor.fetchone()[0]

    cursor.execute(
        'INSERT INTO campaign (campaign_type,name,deployment,start_location,end_location,start_time,end_time,urls) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING campaign_id',
        ['cruise', obj['name'][0], obj['deployment'][0], obj['start_location'][0], obj['end_location'][0], obj['start_time'][0], obj['end_time'][0], obj['urls']]
    )
    return cursor.fetchone()[0]


def load_sampling_events(db, package):
    resources = get_resources_by_type("sampling_event", package.resources)
    if not resources:
        raise Exception("No sampling_event resource found")

    allSamplingEvents = []
    for i in range(len(resources)):
        resource = resources[i]
        print("Sampling event:", resource.name)
        samplingEvents = load_resource(db, resource, SAMPLING_EVENT_DB_SCHEMA, "sampling_event", insert_sampling_event)
        allSamplingEvents.append(samplingEvents)
        db.commit()

    return allSamplingEvents


def insert_sampling_event(db, tableName, obj):
    cursor = db.cursor()

    if not obj['sampling_event_id']:
        raise Exception("Missing sampling event name")
    samplingEventId = obj['sampling_event_id'][0]

    if obj['sampling_event_type']:
        samplingEventType = obj['sampling_event_type'][0]
    else:
        samplingEventType = "Unknown"

    if obj['campaign_id']:
        campaignAccn = obj['campaign_id'][0]
    else:
        campaignAccn = None

    campaignId = None
    if campaignAccn:
        cursor.execute('SELECT campaign_id FROM campaign WHERE name=%s', [campaignAccn])
        if cursor.rowcount > 0:
            campaignId = cursor.fetchone()[0]

    latitudeVals = []
    longitudeVals = []
    if obj['latitude'] and obj['longitude']:
        latitudeVals.append(obj['latitude'][0])
        longitudeVals.append(obj['longitude'][0])
    if obj['start_latitude'] and obj['start_longitude']:
        latitudeVals.append(obj['start_latitude'][0])
        longitudeVals.append(obj['start_longitude'][0])
    if obj['end_latitude'] and obj['end_longitude']:
        latitudeVals.append(obj['end_latitude'][0])
        longitudeVals.append(obj['end_longitude'][0])

    if len(latitudeVals) != len(longitudeVals):
        raise Exception("Mismatched lat/lng values for sampling event")

    for lat in latitudeVals:
        if not valid_latitude(lat):
            raise Exception("Invalid latitude value:", lat, samplingEventId)

    for lng in longitudeVals:
        if not valid_longitude(lng):
            raise Exception("Invalid longitde value:", lng, samplingEventId)

    if obj['start_time']:
        startTime = obj['start_time'][0]
    else:
        startTime = None

    locations = MultiPoint(list(zip(longitudeVals, latitudeVals)))

    cursor.execute('SELECT sampling_event_id FROM sampling_event WHERE name=%s', [samplingEventId])
    if cursor.rowcount > 0:
        return cursor.fetchone()[0]

    cursor.execute(
        'INSERT INTO sampling_event (name,sampling_event_type,campaign_id,locations,start_time) VALUES (%s,%s,%s,ST_SetSRID(%s::geography, 4326),%s) RETURNING sampling_event_id',
        [samplingEventId, samplingEventType, campaignId, locations.wkb_hex if len(locations) else None, startTime]
    )
    return cursor.fetchone()[0]


def load_samples(db, package, sampling_events):
    resources = get_resources_by_type("sample", package.resources)
    if not resources:
        raise Exception("No sample resources found")

    # Join schema and data for all sample resources
    allFields, valuesBySampleId, sampleIdToSampleEventId = join_samples(resources)

    # Load schema
    schema_id = insert_schema(db, package.descriptor['name'], { "fields": allFields })

    # Load sample values
    cursor = db.cursor()
    samples = {}
    count = 0
    for sampleId in valuesBySampleId:
        latitudeVals = []
        longitudeVals = []
        numberVals = []
        stringVals = []
        datetimeVals = []
        for f in allFields:
            name = f['name']
            type = f['type']
            rdfType = f['rdfType']
            #unitRdfType = f['pm:unitRdfType']
            searchable = f['pm:searchable']

            key = fieldUniqueKey(f)
            if key in valuesBySampleId[sampleId]:
                val = valuesBySampleId[sampleId][key]
            else:
                val = None

            if type == 'number':
                if val == None:  # no data
                    numberVals.append(None)
                else:
                    try:
                        val = float(val) #convert_units(unitRdfType, float(val))
                    except ValueError:
                        print("Error converting %s to float at column %s in sample %s" % (val, name, sampleId))
                        raise
                    numberVals.append(val)

                    try:
                        if LATITUDE_PURLS.index(rdfType) >= 0 and searchable:
                            latitudeVals.append(val)
                    except ValueError:
                        pass
                    try:
                        if LONGITUDE_PURLS.index(rdfType) >= 0 and searchable:
                            longitudeVals.append(val)
                    except ValueError:
                        pass
                stringVals.append(None)
                datetimeVals.append(None)
            elif type == 'string' or type == 'duration': #FIXME should convert duration into something searchable
                stringVals.append(str(val))
                numberVals.append(None)
                datetimeVals.append(None)
            elif type == 'datetime' or type == 'date':  # TODO handle time zone # TODO handle type 'time'
                stringVals.append(None)
                numberVals.append(None)
                datetimeVals.append(val)
            else: # TODO throw error
                print("\nUnknown type:", type)
                stringVals.append(None)
                numberVals.append(None)
                datetimeVals.append(None)

        if len(latitudeVals) != len(longitudeVals):
            raise Exception("Mismatched lat/lng values for sampling event")

        for lat in latitudeVals:
            if not valid_latitude(lat):
                raise Exception("Invalid latitude value:", lat, sampleId)

        for lng in longitudeVals:
            if not valid_longitude(lng):
                raise Exception("Invalid longitde value:", lng, sampleId)

        locations = MultiPoint(list(zip(longitudeVals, latitudeVals)))

        stmt = cursor.mogrify(
            "INSERT INTO sample (schema_id,accn,locations,number_vals,string_vals,datetime_vals) VALUES(%s,%s,ST_SetSRID(%s::geography, 4326),%s,%s,%s::timestamp[]) RETURNING sample_id",
            [schema_id, sampleId, locations.wkb_hex if len(locations) else None, numberVals, stringVals, datetimeVals]
        )
        cursor.execute(stmt)
        sample_id = cursor.fetchone()[0]
        samples[sample_id] = valuesBySampleId[sampleId]

        # Link sample to sampling events
        if sampleId in sampleIdToSampleEventId:
            for eventId in sampleIdToSampleEventId[sampleId]:
                for events in sampling_events:
                    for eventId2 in events:
                        if events[eventId2]['sampling_event_id'][0] == eventId:
                            stmt = cursor.mogrify(
                                "INSERT INTO sample_to_sampling_event (sample_id,sampling_event_id) VALUES(%s,%s) ON CONFLICT(sample_id,sampling_event_id) DO NOTHING",
                                [sample_id, eventId2]
                            )
                            cursor.execute(stmt)
                            break

        count += 1
        print('\rLoading samples', count, end='')
    print()
    db.commit()

    return samples


def join_samples(resources):
    allFields = []
    sampleIdToSampleEventId = {}
    fieldSeen = {}
    valuesBySampleId = {}

    for resource in resources:
        print("Sample resource:", resource.name)

        # Each resource must have a sample ID field to join on
        fields = resource.schema.descriptor['fields']
        rdfTypes = list(map(lambda f: f['rdfType'], fields))
        if not SAMPLE_ID_PURL in rdfTypes:
            raise Exception("Missing sample identifier")
        sampleIdPos = rdfTypes.index(SAMPLE_ID_PURL)

        # Find optional sample event ID
        if SAMPLE_EVENT_ID_PURL in rdfTypes:
            sampleEventIdPos = rdfTypes.index(SAMPLE_EVENT_ID_PURL)
        else:
            sampleEventIdPos = None

        # Append only new fields not yet seen
        for i in range(len(fields)):
            f = fields[i]
            key = fieldUniqueKey(f)
            if not key in fieldSeen:
                fieldSeen[key] = 1
                allFields.append(f) # maintain order
            else:
                print('Warning: removing redundant field ("' + f['name'] + '" ' + key + ')', 'in resource', resource.name)

        # Append values for new fields
        try:
            for row in resource.read():
                sampleId = row[sampleIdPos]
                if not sampleId:
                    raise Exception("Invalid sample identifier:", row[sampleIdPos])

                if sampleEventIdPos != None:
                    sampleEventIds = row[sampleEventIdPos].split(';') # sample event ID can be semi-colon delimited list
                    sampleIdToSampleEventId[sampleId] = sampleEventIds

                for i in range(len(fields)):
                    f = fields[i]
                    key = fieldUniqueKey(f)

                    if not sampleId in valuesBySampleId:
                        valuesBySampleId[sampleId] = {}

                    if not key in valuesBySampleId[sampleId]:
                        valuesBySampleId[sampleId][key] = row[i]
                    elif row[i] != valuesBySampleId[sampleId][key]:
                        if row[i] != None:
                            if valuesBySampleId[sampleId][key] == None:
                                valuesBySampleId[sampleId][key] = row[i]
                            elif f['pm:searchable']:
                                print("Warning: value mismatch!!!!", f['name'], key, sampleId, row[i], "!=", valuesBySampleId[sampleId][key], 'in resource', resource.name)

        except Exception as e:
            print(e)
            if e.errors:
                print(*e.errors, sep='\n')

    return allFields, valuesBySampleId, sampleIdToSampleEventId


def fieldUniqueKey(field):
    rdfType = field['name']
    if 'rdfType' in field and field['rdfType']:
        rdfType = field['rdfType']

    sourceUrl = 'unknown'
    if 'pm:sourceUrl' in field and field['pm:sourceUrl']:
        sourceUrl = field['pm:sourceUrl']

    measurementSourceRdfType = 'unknown'
    if 'pm:measurementSourceRdfType' in field and field['pm:measurementSourceRdfType']:
        measurementSourceRdfType = field['pm:measurementSourceRdfType']

    return field['type'] + ' ' + str(field['pm:searchable']) + ' ' + rdfType + ' ' + sourceUrl + ' ' + measurementSourceRdfType


# def convert_units(unitRdfType, val):
#     if unitRdfType == "http://purl.obolibrary.org/obo/UO_0000027":
#         home = Q_(val, ureg.kelvin)
#         print("convert: ", val, home.to('degC').magnitude)
#         return home.to('degC').magnitude
#
#     return val


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
    accn = package.descriptor['name']
    title = package.descriptor['title']
    description = package.descriptor['description']
    homepage = package.descriptor['homepage']

    cursor.execute(
        'INSERT INTO project (project_type_id,accn,name,description,url) VALUES (%s,%s,%s,%s,%s) RETURNING project_id',
        [project_type_id, accn, title, description, homepage]
    )
    project_id = cursor.fetchone()[0]
    print("Added project", project_id)

    # Create project_to_sample entries
    for sampleId in samples:
        cursor.execute('INSERT INTO project_to_sample (project_id,sample_id) VALUES (%s,%s)', [project_id, sampleId])

    db.commit()
    return project_id


def store_niskin_and_ctd(db, projectId, packagePath, irodsPath, package):
    cursor = db.cursor()
    projectPath = irodsPath + "/" + str(projectId)
    imkdir(projectPath)

    ctdFileTypeId = insert_file_type(db, "CTD Profile")
    niskinFileTypeId = insert_file_type(db, "Niskin Profile")
    tsvFileFormatId = insert_file_format(db, "TSV")

    resources = get_resources_by_type("ctd", package.resources)
    for r in resources:
        path = r.descriptor['path']
        destPath = os.path.dirname(projectPath + "/" + path)
        imkdir(destPath)
        iput(packagePath + "/" + path, destPath)

        cursor.execute(
            'INSERT INTO file (file_type_id,file_format_id,url) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING RETURNING file_id',
            [ctdFileTypeId, tsvFileFormatId, projectPath + "/" + path]
        )
        fileId = cursor.fetchone()[0]

        cursor.execute(
            'INSERT INTO project_to_file (project_id,file_id) VALUES (%s,%s) ON CONFLICT DO NOTHING',
            [projectId, fileId]
        )

    resources = get_resources_by_type("niskin", package.resources)
    for r in resources:
        path = r.descriptor['path']
        destPath = os.path.dirname(projectPath + "/" + path)
        imkdir(destPath)
        iput(packagePath + "/" + path, destPath)

        cursor.execute(
            'INSERT INTO file (file_type_id,file_format_id,url) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING RETURNING file_id',
            [niskinFileTypeId, tsvFileFormatId, projectPath + "/" + path]
        )
        fileId = cursor.fetchone()[0]

        cursor.execute(
            'INSERT INTO project_to_file (project_id,file_id) VALUES (%s,%s) ON CONFLICT DO NOTHING',
            [projectId, fileId]
        )

    db.commit()


def iput(srcPath, destPath):
    print("Transferring to IRODS", srcPath, destPath)
    try:
        subprocess.run(["iput", "-Tf", srcPath, destPath])
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


def imkdir(path):
    try:
        subprocess.run(["imkdir", "-p", path])
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


def insert_file_type(db, name):
    cursor = db.cursor()
    cursor.execute("INSERT INTO file_type (name) VALUES (%s) ON CONFLICT(name) DO UPDATE SET name=EXCLUDED.name RETURNING file_type_id", [name])
    return cursor.fetchone()[0]


def insert_file_format(db, name):
    cursor = db.cursor()
    cursor.execute("INSERT INTO file_format (name) VALUES (%s) ON CONFLICT(name) DO UPDATE SET name=EXCLUDED.name RETURNING file_format_id", [name])
    return cursor.fetchone()[0]


def valid_latitude(lat):
    return (lat >= -90 and lat <= 90)


def valid_longitude(lng):
    return (lng >= -180 and lng <= 180)


def delete_all(db):
    print("Deleting all tables ...")
    cursor = db.cursor()
    cursor.execute("DELETE FROM project_to_sample; DELETE FROM sample_to_sampling_event; DELETE FROM project_to_file; DELETE FROM run_to_file; DELETE FROM file; DELETE FROM file_type; DELETE FROM file_format; DELETE FROM run; DELETE FROM library; DELETE FROM experiment; DELETE FROM sample; DELETE FROM project; DELETE FROM schema; DELETE FROM sampling_event; DELETE FROM campaign;")
    db.commit()


def main(args=None):
    conn = psycopg2.connect(host='', dbname=args['dbname'], user=args['username'], password=args['password'] if 'password' in args else None)

    if 'deleteall' in args:
        delete_all(conn)

    for filepath in args['filepath']:
        package = Package(filepath)
        print('Package:', package.descriptor['name'], '(' + filepath + ')')
        if not package.valid:
            print(package.errors)

        campaigns = load_campaigns(conn, package)
        sampling_events = load_sampling_events(conn, package)
        samples = load_samples(conn, package, sampling_events)
        projectId = insert_project(conn, package, samples)
        if 'irodspath' in args:
            store_niskin_and_ctd(conn, projectId, os.path.dirname(filepath), args['irodspath'], package)
        else:
            print("Skipping store of CTD and Niskin files (see --irodspath option)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load datapackage into database.')
    parser.add_argument('-d', '--dbname')
    parser.add_argument('-u', '--username')
    parser.add_argument('-p', '--password')
    parser.add_argument('-r', '--resource')
    parser.add_argument('-x', '--deleteall', action='store_true')
    parser.add_argument('-i', '--irodspath') # optional IRODS path to store CTD and Niskin files
    parser.add_argument('filepath', nargs='+')

    main(args={k: v for k, v in vars(parser.parse_args()).items() if v})

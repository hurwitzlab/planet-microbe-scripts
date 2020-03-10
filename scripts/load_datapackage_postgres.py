#!/usr/bin/env python3
"""
Load Data Package into Postgres database

load_datapackage_postgres.py -d <database> -u <username> -p <password> <path_to_datapackage.json>
"""

import sys
import os
import argparse
import logging
import subprocess
import csv
import psycopg2
import simplejson as json
from datapackage import Package, Resource
from tableschema import Table
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

        fieldMap[k] = fields

    results = {}
    try:
        i = 0
        for row in resource.iter(keyed=True):
            i += 1
            obj = {}
            for k,fields in fieldMap.items():
                vals = list(filter(lambda v: v != None, map(lambda f: row[f.name], fields)))
                obj[k] = vals

            if len(obj.keys()) != len(fieldMap):
                raise Exception('Error: row length mismatch at row {:d}: {:d} != {:d}'.format(i, len(obj.keys()), len(fieldMap)))

            id = insertMethod(db, tableName, obj)
            results[id] = obj
    except Exception as e:
        print(e)
        if hasattr(e, 'errors'):
            print(*e.errors, sep='\n')
        raise

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


def load_sampling_event_data(db, package, samplingEvents):
    resources = get_resources_by_type("ctd", package.resources)
    resources += get_resources_by_type("niskin", package.resources)

    # Load unit conversions file
    unitMap = load_unit_conversions('./unit_conversions.tsv')  # FIXME hardcoded path

    for resource in resources:
        print("Sampling event data:", resource.name)

        # Load schema
        fields = resource.schema.descriptor['fields']
        rdfTypes = list(map(lambda f: f['rdfType'], fields))
        if not SAMPLE_EVENT_ID_PURL in rdfTypes:
            raise Exception("Missing sampling event identifier (" + SAMPLE_EVENT_ID_PURL + ") for resource " + resource.name)
        sampleEventIdPos = rdfTypes.index(SAMPLE_EVENT_ID_PURL)

        schemaName = package.descriptor['name'] + ' - ' + resource.name
        schemaType = resource.descriptor['pm:resourceType']
        schemaId = insert_schema(db, schemaName, schemaType, {"fields": fields})

        # Load data
        cursor = db.cursor()
        count = 0
        try:
            for row in resource.iter():
                sampleEventId = row[sampleEventIdPos]
                if not sampleEventId:
                    raise Exception("Invalid sampling event identifier (" + sampleEventId + ") for resource " + resource.name)

                numberVals = []
                stringVals = []
                datetimeVals = []
                for i in range(len(fields)):
                    f = fields[i]
                    name = f['name']
                    type = f['type']
                    rdfType = f['rdfType']
                    unitRdfType = f['pm:unitRdfType']

                    val = row[i]

                    if type == 'number':
                        if val == None:  # no data
                            numberVals.append(None)
                        else:
                            try:
                                _, val = convert_units(unitMap, rdfType, unitRdfType, float(val))
                            except ValueError:
                                print("Error converting '%s' to float at column %s in sample %s" % (val, name, sampleId))
                                raise
                            numberVals.append(val)
                        stringVals.append(None)
                        datetimeVals.append(None)
                    elif type == 'string' or type == 'duration' or type == 'time':  # FIXME should convert duration/time into something searchable?
                        stringVals.append(str(val))
                        numberVals.append(None)
                        datetimeVals.append(None)
                    elif type == 'datetime' or type == 'date':  # assume UTC time zone
                        stringVals.append(None)
                        numberVals.append(None)
                        datetimeVals.append(val)
                    else:  # TODO throw error
                        print("\nUnknown type:", type)
                        stringVals.append(None)
                        numberVals.append(None)
                        datetimeVals.append(None)

                samplingEventDbId = None
                for event in samplingEvents:
                    for id in event.keys():
                        if sampleEventId in event[id]['sampling_event_id']:
                            samplingEventDbId = id
                if not samplingEventDbId:
                    raise Exception("Sampling event not found (" + sampleEventId + ") for resource " + resource.name)

                stmt = cursor.mogrify(
                    "INSERT INTO sampling_event_data (sampling_event_id,schema_id,number_vals,string_vals,datetime_vals) "
                    "VALUES(%s,%s,%s,%s,%s::timestamp[]) "
                    "RETURNING sampling_event_data_id",
                    [samplingEventDbId, schemaId, numberVals, stringVals, datetimeVals]
                )
                cursor.execute(stmt)

                count += 1
                print('\rLoading', schemaType, count, end='')

        except Exception as e:
            print(e)
            if hasattr(e, 'errors'):
                print(*e.errors, sep='\n')
            raise

        # Update schema with new units
        for f in fields:
            unit = get_preferred_unit(unitMap, f['rdfType'], f['pm:unitRdfType'])
            if unit:
                f['pm:unitRdfType'] = unit['preferredUnitPurl']
        insert_schema(db, schemaName, schemaType, {'fields': fields})

        db.commit()


def load_samples(db, package, sampling_events):
    resources = get_resources_by_type("sample", package.resources)
    if not resources:
        raise Exception("No sample resources found")

    # Load unit conversions file
    unitMap = load_unit_conversions('./unit_conversions.tsv') #FIXME hardcoded path

    # Join schema and data for all sample resources
    allFields, valuesBySampleId, sampleIdToSampleEventId = join_samples(resources)

    # Load schema
    schemaName = package.descriptor['name'] + ' - samples'
    schemaId = insert_schema(db, schemaName, 'sample', { "fields": allFields })

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
            unitRdfType = f['pm:unitRdfType']
            #searchable = f['pm:searchable']

            key = field_unique_key(f)
            if key in valuesBySampleId[sampleId]:
                val = valuesBySampleId[sampleId][key]
            else:
                val = None

            if type == 'number':
                if val == None:  # no data
                    numberVals.append(None)
                else:
                    try:
                        _, val = convert_units(unitMap, rdfType, unitRdfType, float(val))
                    except ValueError:
                        print("Error converting '%s' to float at column %s in sample %s" % (val, name, sampleId))
                        raise
                    numberVals.append(val)

                    try:
                        if LATITUDE_PURLS.index(rdfType) >= 0: # and searchable:
                            latitudeVals.append(val)
                    except ValueError:
                        pass
                    try:
                        if LONGITUDE_PURLS.index(rdfType) >= 0: # and searchable:
                            longitudeVals.append(val)
                    except ValueError:
                        pass
                stringVals.append(None)
                datetimeVals.append(None)
            elif type == 'string' or type == 'duration' or type == 'time': #FIXME should convert duration/time into something searchable?
                stringVals.append(str(val))
                numberVals.append(None)
                datetimeVals.append(None)
            elif type == 'datetime' or type == 'date':  # assume UTC time zone
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

        if not validate_coords(longitudeVals):
            logging.warning("Longitude coordinates %s do not match within threshold for sample %s", longitudeVals, sampleId)
        if not validate_coords(latitudeVals):
            logging.warning("Latitude coordinates %s do not match within threshold for sample %s", latitudeVals, sampleId)
        locations = MultiPoint(list(zip(longitudeVals, latitudeVals)))

        stmt = cursor.mogrify(
            "INSERT INTO sample (schema_id,accn,locations,number_vals,string_vals,datetime_vals) "
            "VALUES(%s,%s,ST_SetSRID(%s::geography, 4326),%s,%s,%s::timestamp[]) "
            "RETURNING sample_id",
            [schemaId, sampleId,
             locations.wkb_hex if len(locations) else None,
             numberVals, stringVals, datetimeVals]
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
                                "INSERT INTO sample_to_sampling_event (sample_id,sampling_event_id) "
                                "VALUES(%s,%s) ON CONFLICT(sample_id,sampling_event_id) DO NOTHING",
                                [sample_id, eventId2]
                            )
                            cursor.execute(stmt)
                            break

        count += 1
        print('\rLoading samples', count, end='')
    print()

    # Update schema with new units
    for f in allFields:
        unit = get_preferred_unit(unitMap, f['rdfType'], f['pm:unitRdfType'])
        if unit:
            f['pm:unitRdfType'] = unit['preferredUnitPurl']
    insert_schema(db, schemaName, 'sample', { 'fields': allFields })

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
            raise Exception("Missing sample identifier (" + SAMPLE_ID_PURL + ") for resource " + resource.name)
        sampleIdPos = rdfTypes.index(SAMPLE_ID_PURL)

        # Find optional sample event ID
        if SAMPLE_EVENT_ID_PURL in rdfTypes:
            sampleEventIdPos = rdfTypes.index(SAMPLE_EVENT_ID_PURL)
        else:
            sampleEventIdPos = None

        # Alias "below detection limit" values
        bdlValues = []
        if 'belowDetectionLimitValues' in resource.schema.descriptor:
            bdlValues = resource.schema.descriptor['belowDetectionLimitValues']

        # Append fields
        for i in range(len(fields)):
            f = fields[i]

            # Prevent redundant fields
            key = field_unique_key(f)
            if key in fieldSeen:
                logging.warning('removing redundant field %s (%s) in resource %s', f['name'], key, resource.name)
                continue

            # Append new field, maintaining the order
            fieldSeen[key] = 1
            allFields.append(f)

        # Append values for fields
        try:
            lineNum = 0
            for row in resource.iter(cast=False):
                lineNum += 1

                # Handle "Below Detection Limit" values
                # This is why casting is disabled in line above.  Have to manually cast here.
                for i in range(len(row)):
                    if row[i] in bdlValues:
                        row[i] = float('nan')
                row = resource.schema.cast_row(row)

                # Verify Sample ID value
                sampleId = row[sampleIdPos]
                if not sampleId:
                    raise Exception("Invalid sample ID value " + str(row[sampleIdPos]) + " on line " + str(lineNum) + " in resource " + resource.name)

                # Get Sample Event ID(s)
                if sampleEventIdPos != None:
                    if not row[sampleEventIdPos]:
                        raise Exception("Invalid sample event ID value " + str(row[sampleEventIdPos]) + " on line " + str(lineNum) + " in resource " + resource.name)
                    sampleEventIds = row[sampleEventIdPos].split(';') # sample event ID can be semi-colon delimited list
                    sampleIdToSampleEventId[sampleId] = sampleEventIds

                # Index values by unique signature to remove duplicate fields
                for i in range(len(fields)):
                    f = fields[i]
                    key = field_unique_key(f)
                    val = row[i]

                    if not sampleId in valuesBySampleId:
                        valuesBySampleId[sampleId] = {}

                    if not key in valuesBySampleId[sampleId]:
                        valuesBySampleId[sampleId][key] = val
                    elif val != valuesBySampleId[sampleId][key]:
                        if val != None:
                            if valuesBySampleId[sampleId][key] == None:
                                valuesBySampleId[sampleId][key] = val
                            else: #elif f['pm:searchable']:
                                logging.warning('value mismatch: key "%s" sample %s val != %s in resource %s', key, sampleId, valuesBySampleId[sampleId][key], resource.name)
        except Exception as e:
            print(e)
            if hasattr(e, 'errors'):
                print(*e.errors, sep='\n')
            raise

    return allFields, valuesBySampleId, sampleIdToSampleEventId


def field_unique_key(field):
    rdfType = field['name']
    if 'rdfType' in field and field['rdfType']:
        rdfType = field['rdfType']

    # Prevent multiple Sample ID fields regardless of source
    if rdfType == SAMPLE_ID_PURL:
        return field['type'] + ' ' + rdfType

    sourceUrl = 'unknown'
    if 'pm:sourceUrl' in field and field['pm:sourceUrl']:
        sourceUrl = field['pm:sourceUrl']

    measurementSourceRdfType = 'unknown'
    if 'pm:measurementSourceRdfType' in field and field['pm:measurementSourceRdfType']:
        measurementSourceRdfType = field['pm:measurementSourceRdfType']

    return field['type'] + ' ' + rdfType + ' ' + sourceUrl + ' ' + measurementSourceRdfType


def validate_coords(coords):
    THRESHOLD = 5 # degrees
    for i in range(len(coords) - 1):
        for j in range(1, len(coords)):
            if i != j and abs(coords[i] - coords[j]) > THRESHOLD:
                return False
    return True


def convert_units(unitMap, purl, sourceUnitPurl, val):
    unit = get_preferred_unit(unitMap, purl, sourceUnitPurl)
    if unit:
        preferredUnitPurl = unit['preferredUnitPurl']
        conversionFactor = unit['conversionFactor']
        newVal = val * conversionFactor
        logging.debug('Converting %s to %s: %s to %s for %s', sourceUnitPurl, preferredUnitPurl, val, newVal, purl)
        return preferredUnitPurl, newVal

    return sourceUnitPurl, val


def get_preferred_unit(unitMap, purl, sourceUnitPurl):
    if sourceUnitPurl in unitMap['*']:
        purl = '*'
    if purl in unitMap and sourceUnitPurl in unitMap[purl]:
        return unitMap[purl][sourceUnitPurl]

    return


def load_unit_conversions(path):
    unitMap = {}
    with open(path, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for row in reader:
            if len(row) == 0 or row[0] == '' or row[0].startswith('#'):
                continue

            purl = row[0]
            preferredUnitPurl = row[1]
            sourceUnitPurl = row[2]
            conversionFactor = row[3]
            if not purl in unitMap:
                unitMap[purl] = {}
            unitMap[purl][sourceUnitPurl] = {
                'preferredUnitPurl': preferredUnitPurl,
                'conversionFactor': float(conversionFactor)
            }
    return unitMap


def insert_schema(db, name, type, fields):
    cursor = db.cursor()
    print('Loading schema "%s"' % name)
    cursor.execute('INSERT INTO schema (name,type,fields) VALUES (%s,%s,%s) ON CONFLICT(name) DO UPDATE SET name=EXCLUDED.name,fields=EXCLUDED.fields RETURNING schema_id',
                   [name, type, json.dumps(fields)])
    schemaId = cursor.fetchone()[0]
    db.commit()
    print("Added schema", schemaId)
    return schemaId


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
    datapackageUrl = package.descriptor['pm:selfUrl']

    cursor.execute(
        'INSERT INTO project (project_type_id,accn,name,description,url,datapackage_url) VALUES (%s,%s,%s,%s,%s,%s) RETURNING project_id',
        [project_type_id, accn, title, description, homepage, datapackageUrl]
    )
    project_id = cursor.fetchone()[0]
    print("Added project", project_id)

    # Create project_to_sample entries
    for sampleId in samples:
        cursor.execute('INSERT INTO project_to_sample (project_id,sample_id) VALUES (%s,%s)', [project_id, sampleId])

    db.commit()
    return project_id, title


def store_niskin_and_ctd(db, projectId, packagePath, targetPath, package):
    cursor = db.cursor()

    ctdFileTypeId = insert_file_type(db, "CTD Profile")
    niskinFileTypeId = insert_file_type(db, "Niskin Profile")
    tsvFileFormatId = insert_file_format(db, "TSV")

    resources = get_resources_by_type("ctd", package.resources)
    for r in resources:
        path = r.descriptor['path']
        destPath = os.path.dirname(targetPath + "/" + path)
        imkdir(destPath)
        iput(packagePath + "/" + path, destPath)

        cursor.execute(
            'INSERT INTO file (file_type_id,file_format_id,url) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING RETURNING file_id',
            [ctdFileTypeId, tsvFileFormatId, targetPath + "/" + path]
        )
        fileId = cursor.fetchone()[0]

        cursor.execute(
            'INSERT INTO project_to_file (project_id,file_id) VALUES (%s,%s) ON CONFLICT DO NOTHING',
            [projectId, fileId]
        )

    resources = get_resources_by_type("niskin", package.resources)
    for r in resources:
        path = r.descriptor['path']
        destPath = os.path.dirname(targetPath + "/" + path)
        imkdir(destPath)
        iput(packagePath + "/" + path, destPath)

        cursor.execute(
            'INSERT INTO file (file_type_id,file_format_id,url) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING RETURNING file_id',
            [niskinFileTypeId, tsvFileFormatId, targetPath + "/" + path]
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
    cursor.execute(
        "DELETE FROM project_to_sample;"
        "DELETE FROM sample_to_sampling_event;"
        "DELETE FROM project_to_file;"
        "DELETE FROM run_to_file;"
        "DELETE FROM file;"
        "DELETE FROM file_type;"
        "DELETE FROM file_format;"
        "DELETE FROM centrifuge;" 
        "DELETE FROM taxonomy;"
        "DELETE FROM run;"
        "DELETE FROM library;"
        "DELETE FROM experiment;"
        "DELETE FROM sample;"
        "DELETE FROM project;"
        "DELETE FROM sampling_event_data;"
        "DELETE FROM sampling_event;"
        "DELETE FROM schema;"
        "DELETE FROM campaign;"
    )
    db.commit()


def main(args=None):
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    logger = logging.getLogger()
    if 'nowarn' in args:
        logger.setLevel(logging.ERROR)
    elif 'debug' in args:
        logger.setLevel(logging.DEBUG)

    conn = psycopg2.connect(host='', dbname=args['dbname'], user=args['username'], password=args['password'] if 'password' in args else None)

    if 'deleteall' in args:
        delete_all(conn)

    for filepath in args['filepath']:
        package = Package(filepath)
        print('Package:', package.descriptor['name'], '(' + filepath + ')')
        if not package.valid:
            print(package.errors)

        campaigns = load_campaigns(conn, package)
        samplingEvents = load_sampling_events(conn, package)
        load_sampling_event_data(conn, package, samplingEvents)
        samples = load_samples(conn, package, samplingEvents)
        projectId, projectTitle = insert_project(conn, package, samples)
        if 'irodspath' in args and args['irodspath']:
            targetPath = args['irodspath'] + '/' + projectTitle.replace(' ', '_')
            store_niskin_and_ctd(conn, projectId, os.path.dirname(filepath), targetPath, package)
        else:
            print("Skipping store of CTD and Niskin files (see --irodspath option)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load datapackage into database.')
    parser.add_argument('-d', '--dbname')
    parser.add_argument('-u', '--username')
    parser.add_argument('-p', '--password')
    parser.add_argument('-x', '--deleteall', action='store_true')
    parser.add_argument('-i', '--irodspath')                      # optional IRODS path to store CTD and Niskin files
    parser.add_argument('--debug', action='store_true')           # show debug messages
    parser.add_argument('--nowarn', action='store_true')          # suppress all warnings
    parser.add_argument('filepath', nargs='+')

    main(args={k: v for k, v in vars(parser.parse_args()).items() if v})

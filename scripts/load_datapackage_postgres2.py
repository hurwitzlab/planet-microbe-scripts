#!/usr/bin/env python3
"""
Load Data Package schema and data into Postgres database

load_datapackage_postgres2.py -d <database> -u <username> -p <password> datapackage.json
"""

import sys
import argparse
import simplejson as json
from datapackage import Package, Resource
from tableschema import Table
import psycopg2
from shapely.geometry import MultiPoint
from shapely import wkb
from pint import UnitRegistry
from Bio import Entrez
import xml.etree.ElementTree as ET


ureg = UnitRegistry()
Q_ = ureg.Quantity

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
    elif len(resources) > 1:
        raise Exception("More than one campaign resource found")

    print("Campaign resource:", resources[0].name)
    campaigns = load_resource(db, resources[0], CAMPAIGN_CRUISE_DB_SCHEMA, "campaign", insert_campaign)
    db.commit()
    return campaigns


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
    cursor.execute(
        'INSERT INTO campaign (campaign_type,name,deployment,start_location,end_location,start_time,end_time,urls) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING campaign_id',
        ['cruise', obj['name'][0], obj['deployment'][0], obj['start_location'][0], obj['end_location'][0], obj['start_time'][0], obj['end_time'][0], obj['urls']]
    )
    return cursor.fetchone()[0]


def load_sampling_events(db, package):
    resources = get_resources_by_type("sampling_event", package.resources)
    if not resources:
        raise Exception("No sampling_event resource found")
    elif len(resources) > 1:
        raise Exception("More than one sampling_event resource found")

    print("Sampling event:", resources[0].name)
    sampling_events = load_resource(db, resources[0], SAMPLING_EVENT_DB_SCHEMA, "sampling_event", insert_sampling_event)
    db.commit()
    return sampling_events


def insert_sampling_event(db, tableName, obj):
    cursor = db.cursor()
    cursor.execute('SELECT campaign_id FROM campaign WHERE name=%s', obj['campaign_id']) # FIXME campaign name may not be unique
    if cursor.rowcount > 0:
        campaignId = cursor.fetchone()[0]
    else:
        campaignId = None

    if obj['sampling_event_id']:
        samplingEventId = obj['sampling_event_id'][0]
    else:
        samplingEventId = None

    if obj['sampling_event_type']:
        samplingEventType = obj['sampling_event_type'][0]
    else:
        samplingEventType = "Unknown"

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

    if len(latitudeVals) == 0 or len(longitudeVals) == 0 or len(latitudeVals) != len(longitudeVals):
        raise Exception("Invalid lat/lng values for sampling event")

    # print("accn:", samplingEventId)
    # print("lats:", latitudeVals)
    # print("lngs:", longitudeVals)
    # print("zip:", list(zip(longitudeVals, latitudeVals)))
    locations = MultiPoint(list(zip(longitudeVals, latitudeVals)))

    #print('Loading', samplingEventId)
    cursor.execute(
        'INSERT INTO sampling_event (name,sampling_event_type,campaign_id,locations,start_time,data_url) VALUES (%s,%s,%s,ST_SetSRID(%s::geography, 4326),%s,%s) RETURNING sampling_event_id',
        [samplingEventId, samplingEventType, campaignId, locations.wkb_hex, obj['start_time'][0], "FIXME"]
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
        print("Sample resource:", resource.name)
        # for f in resource.schema.fields:
        #     print(f)
        #     print(f.name)

        # Determine position of Sample ID fields
        fields = resource.schema.descriptor['fields']
        rdfTypes = list(map(lambda f: f['rdfType'], fields))

        if not SAMPLE_ID_PURL in rdfTypes:
            raise Exception("Missing sample identifier")
        sampleIdPos = rdfTypes.index(SAMPLE_ID_PURL)

        # Index sample event IDs and remove redundant fields
        if SAMPLE_EVENT_ID_PURL in rdfTypes:
            sampleEventIdPos = rdfTypes.index(SAMPLE_EVENT_ID_PURL)
        else:
            sampleEventIdPos = None

        # Remove redundant Sample ID and Sample Event ID fields
        if i > 0:
            del fields[sampleIdPos]
            if sampleEventIdPos != None:
                del fields[sampleEventIdPos]
        allFields.extend(fields)

        try:
            for row in resource.read():
                sampleId = row[sampleIdPos]
                if sampleEventIdPos != None:
                    sampleEventIds = row[sampleEventIdPos].split(';')
                    sampleIdToSampleEventId[sampleId] = sampleEventIds
                if i > 0:
                    del row[sampleIdPos]
                    if sampleEventIdPos != None:
                        del row[sampleEventIdPos]

                if not sampleId in valuesBySampleId:
                    valuesBySampleId[sampleId] = []
                valuesBySampleId[sampleId].extend(row)
        except Exception as e:
            print(e)
            if e.errors:
                print(*e.errors, sep='\n')

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
            unitRdfType = allFields[i]['pm:unitRdfType']
            searchable = allFields[i]['pm:searchable']
            if type == 'number':
                if val == None:  # no data
                    numberVals.append(None)
                else:
                    val = float(val) #convert_units(unitRdfType, float(val))
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
            elif type == 'string':
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
            i += 1

        # sampling_event_id = None
        # if sampleEventIdPos != None:
        #     for eventId in sampling_events:
        #         for eventId2 in sampleIdToSampleEventId:
        #             if sampling_events[eventId]['sampling_event_id'][0] == eventId2:
        #                 sampling_event_id = eventId
        #                 break

        # print("accn:", id)
        # print("lats:", latitudeVals)
        # print("lngs:", longitudeVals)
        # print("zip:", list(zip(longitudeVals, latitudeVals)))
        locations = MultiPoint(list(zip(longitudeVals, latitudeVals)))

        stmt = cursor.mogrify(
            "INSERT INTO sample (schema_id,accn,locations,number_vals,string_vals,datetime_vals) VALUES(%s,%s,ST_SetSRID(%s::geography, 4326),%s,%s,%s::timestamp[]) RETURNING sample_id",
            [schema_id, id, locations.wkb_hex, numberVals, stringVals, datetimeVals]
        )
        cursor.execute(stmt)
        sample_id = cursor.fetchone()[0]
        samples[sample_id] = valuesBySampleId[id]

        if id in sampleIdToSampleEventId:
            for eventId in sampleIdToSampleEventId[id]:
                for eventId2 in sampling_events:
                    if sampling_events[eventId2]['sampling_event_id'][0] == eventId:
                        #print("Linking", sample_id, eventId2)
                        stmt = cursor.mogrify(
                            "INSERT INTO sample_to_sampling_event (sample_id,sampling_event_id) VALUES(%s,%s)",
                            [sample_id, eventId2]
                        )
                        cursor.execute(stmt)
                        break

        db.commit()
        count += 1
        print('\rLoading samples', count, end='')
    print()

    return samples


def convert_units(unitRdfType, val):
    if unitRdfType == "http://purl.obolibrary.org/obo/UO_0000027":
        home = Q_(val, ureg.kelvin)
        print("convert: ", val, home.to('degC').magnitude)
        return home.to('degC').magnitude

    return val


def esearch(db, accn):
    handle = Entrez.esearch(db=db, term=accn)
    result = Entrez.read(handle)
    handle.close()
    if int(result['Count']) == 0:
        raise Exception(db + " accn not found:", accn)
    return result['IdList']


def getSummary(db, accn):
    idList = esearch(db, accn)
    # print(idList)
    handle = Entrez.esummary(db=db, id=','.join(idList), retmode='xml')
    result = Entrez.read(handle)
    handle.close()
    return result


def getExperimentsFromSRA(sampleAccn):
    experiments = []
    print("BioSample accn:", sampleAccn)
    result = getSummary('biosample', sampleAccn)
    docs = result['DocumentSummarySet']['DocumentSummary']
    if len(docs) > 1:
        raise Exception("More than one BioSample result found for", accn, result['IdList'])
    for summary in docs:
        # NCBIXML raises error "AttributeError: 'StringElement' object has no attribute 'read'"
        # for record in NCBIXML.read(summary['SampleData']):
        #     print(record)

        record = ET.fromstring(summary['SampleData'])
        attr = record.find(".//Id[@db='SRA']")
        if attr == None:
            raise Exception("Could not parse SRA accn for BioSample:", sampleAccn)
        sraAccn = attr.text
        # print("sample SRA accn:", sraAccn)

        result = getSummary('sra', sraAccn)
        for record in result:
            doc = ET.fromstring('<root>' + record['ExpXml'] + '</root>')
            exp = doc.find(".//Experiment")
            name = exp.attrib['name']
            accn = exp.attrib['acc']
            # print("experiment accn:", accn)
            # print("experiment name:", name)
            experiment = {
                'accn': accn,
                'name': name,
                'runs': []
            }

            doc = ET.fromstring('<root>' + record['Runs'] + '</root>')
            run = doc.find(".//Run")
            accn = run.attrib['acc']
            totalSpots = run.attrib['total_spots']
            totalBases = run.attrib['total_bases']
            # print("run accn:", accn)
            # print("total spots:", totalSpots)
            # print("total bases:", totalBases)
            experiment['runs'].append({
                'accn': accn,
                'spots': totalSpots,
                'bases': totalBases
            })

            experiments.append(experiment)

    return experiments


def load_experiments(db, key, email, samples):
    cursor = db.cursor()
    Entrez.api_key = key
    Entrez.email = email
    for sampleId in samples:
        cursor.execute('SELECT accn FROM sample WHERE sample_id=%s', [sampleId]) #TODO pull accn directly from sample object
        sampleAccn = cursor.fetchone()[0]

        experiments = getExperimentsFromSRA(sampleAccn)
        print(experiments)
        for exp in experiments:
            cursor.execute('INSERT INTO experiment (sample_id,name,accn) VALUES (%s,%s,%s) RETURNING experiment_id',
                           [sampleId, exp['name'], exp['accn']])
            experimentId = cursor.fetchone()[0]

            for run in exp['runs']:
                cursor.execute('INSERT INTO run (experiment_id,accn,total_spots,total_bases) VALUES (%s,%s,%s,%s) RETURNING experiment_id',
                               [experimentId, run['accn'], run['spots'], run['bases']])

            db.commit()


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


def delete_all(db):
    print("Deleting all tables ...")
    cursor = db.cursor()
    cursor.execute("delete from project_to_sample; delete from sample_to_sampling_event; delete from run; delete from experiment; delete from sample; delete from project; delete from schema; delete from sampling_event; delete from campaign;")
    db.commit()


def main(args=None):
    if 'password' in args:
        conn = psycopg2.connect(host='', dbname=args['dbname'], user=args['username'], password=args['password'])
    else:
        conn = psycopg2.connect(host='', dbname=args['dbname'], user=args['username'])

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
        insert_project(conn, package, samples)

        if 'key' in args and 'email' in args:
            load_experiments(conn, args['key'], args['email'], samples)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load datapackage into database.')
    parser.add_argument('-d', '--dbname')
    parser.add_argument('-u', '--username')
    parser.add_argument('-p', '--password')
    parser.add_argument('-k', '--key')  # For NCBI Entrez calls
    parser.add_argument('-e', '--email') # For NCBI Entrez calls
    parser.add_argument('-r', '--resource')
    parser.add_argument('-x', '--deleteall', action='store_true')
    parser.add_argument('filepath', nargs='+')

    main(args={k: v for k, v in vars(parser.parse_args()).items() if v})

#!/usr/bin/env python3
import sys
import argparse
from Bio import Entrez
# from Bio.Blast import NCBIXML
import xml.etree.ElementTree as ET
import psycopg2


def esearch(db, accn):
    handle = Entrez.esearch(db=db, term=accn)
    result = Entrez.read(handle)
    handle.close()
    if int(result['Count']) == 0:
        raise Exception("BioSample accn not found:", accn)
    if int(result['Count']) > 1:
        raise Exception("More than one BioSample found for", accn)
    return result['IdList'][0]


def getSummary(db, accn):
    id = esearch(db, accn)
    handle = Entrez.esummary(db=db, id=id, retmode='xml')
    result = Entrez.read(handle)
    handle.close()
    return result


def getExperimentsFromSRA(sampleAccn):
    experiments = []
    result = getSummary('biosample', sampleAccn)
    for summary in result['DocumentSummarySet']['DocumentSummary']:
        # NCBIXML raises error "AttributeError: 'StringElement' object has no attribute 'read'"
        # for record in NCBIXML.read(summary['SampleData']):
        #     print(record)

        record = ET.fromstring(summary['SampleData'])
        attr = record.find(".//Id[@db='SRA']")
        if attr == None:
            print(summary['SampleData'])
            raise Exception("Could not parse SRA accn for BioSample:", sampleAccn)
        sraAccn = attr.text
        print("sample SRA accn:", sraAccn)

        result = getSummary('sra', sraAccn)
        for record in result:
            doc = ET.fromstring('<root>' + record['ExpXml'] + '</root>')
            exp = doc.find(".//Experiment")
            name = exp.attrib['name']
            accn = exp.attrib['acc']
            print("experiment accn:", accn)
            print("experiment name:", name)
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
            print("run accn:", accn)
            print("total spots:", totalSpots)
            print("total bases:", totalBases)
            experiment['runs'].append({
                'accn': accn,
                'spots': totalSpots,
                'bases': totalBases
            })

            experiments.append(experiment)

    return experiments


def loadExperiments(db):
    cursor = db.cursor()
    cursor.execute('SELECT sample_id,accn FROM sample')
    for row in cursor.fetchall():
        sampleId = row[0]
        sampleAccn = row[1]

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


def main(args=None):
    if 'accn' in args: # for debug
        getExperimentsFromSRA(args['accn'])
    else: # load all experiments and runs into db
        if 'password' in args:
            conn = psycopg2.connect(host='', dbname=args['dbname'], user=args['username'], password=args['password'])
        else:
            conn = psycopg2.connect(host='', dbname=args['dbname'], user=args['username'])

        if not ('key' in args) or not ('email' in args):
            raise("Missing required key and email args")
        Entrez.api_key = args['key']
        Entrez.email = args['email']

        loadExperiments(conn)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load datapackage into database.')
    parser.add_argument('-d', '--dbname')
    parser.add_argument('-u', '--username')
    parser.add_argument('-p', '--password')
    parser.add_argument('-k', '--key')  # For NCBI Entrez calls
    parser.add_argument('-e', '--email') # For NCBI Entrez calls
    parser.add_argument('-a', '--accn')

    main(args={k: v for k, v in vars(parser.parse_args()).items() if v})

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
        raise Exception(db + " accn not found:", accn)
    return result['IdList']


def getSummary(db, accn):
    idList = esearch(db, accn)
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
        raise Exception("More than one BioSample result found for", sampleAccn, docs)

    for summary in docs:
        # NCBIXML raises error "AttributeError: 'StringElement' object has no attribute 'read'"
        # for record in NCBIXML.read(summary['SampleData']):
        #     print(record)

        record = ET.fromstring(summary['SampleData'])
        attr = record.find(".//Attribute[@attribute_name='SRA accession']")
        if attr == None:
            attr = record.find(".//Id[@db='SRA']")
        if attr == None:
            print(summary['SampleData'])
            raise Exception("Could not parse SRA accn for BioSample:", sampleAccn)
        sraAccn = attr.text
        print("SRA accn:", sraAccn)

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

            name = doc.find(".//Library_descriptor/LIBRARY_NAME")
            strategy = doc.find(".//Library_descriptor/LIBRARY_STRATEGY")
            source = doc.find(".//Library_descriptor/LIBRARY_SOURCE")
            selection = doc.find(".//Library_descriptor/LIBRARY_SELECTION")
            protocol = doc.find(".//Library_descriptor/LIBRARY_CONSTRUCTION_PROTOCOL")
            paired = doc.find(".//Library_descriptor/LIBRARY_LAYOUT/PAIRED")
            single = doc.find(".//Library_descriptor/LIBRARY_LAYOUT/SINGLE")
            if paired != None:
                length = paired.attrib['NOMINAL_LENGTH'] if 'NOMINAL_LENGTH' in paired.attrib else None
                layout = 'paired'
            elif single != None:
                length = single.attrib['NOMINAL_LENGTH'] if 'NOMINAL_LENGTH' in single.attrib else None
                layout = 'single'
            else:
                raise Exception('Missing library layout')
            experiment['library'] = {
                'name': name.text if name != None else None,
                'strategy': strategy.text,
                'source': source.text,
                'selection': selection.text,
                'protocol': protocol.text if protocol != None else None,
                'layout': layout,
                'length': length
            }

            doc = ET.fromstring('<root>' + record['Runs'] + '</root>')
            runs = doc.findall(".//Run")
            for run in runs:
                accn = run.attrib['acc']
                totalSpots = run.attrib['total_spots']
                totalBases = run.attrib['total_bases']
                # print("run accn:", acc, "total spots:", totalSpots, "total bases:", totalBases)
                experiment['runs'].append({
                    'accn': accn,
                    'spots': totalSpots,
                    'bases': totalBases
                })

            experiments.append(experiment)

    #print(experiments)
    return experiments


def loadExperiments(db):
    cursor = db.cursor()
    cursor.execute('SELECT sample_id,accn FROM sample')
    for row in cursor.fetchall():
        sampleId = row[0]
        sampleAccn = row[1]

        experiments = getExperimentsFromSRA(sampleAccn)
        for exp in experiments:
            print("Experiment accn:", exp['accn'])
            cursor.execute('INSERT INTO experiment (sample_id,name,accn) VALUES (%s,%s,%s) RETURNING experiment_id',
                           [sampleId, exp['name'], exp['accn']])
            experimentId = cursor.fetchone()[0]

            lib = exp['library']
            cursor.execute('INSERT INTO library (experiment_id,name,strategy,source,selection,protocol,layout,length) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING library_id',
                           [experimentId, lib['name'], lib['strategy'], lib['source'], lib['selection'], lib['protocol'], lib['layout'], lib['length']])

            for run in exp['runs']:
                if run['spots'] == '' or run['bases'] == '': # added for run ERR1718568 in sample SAMEA2732304 (Tara Oceans)
                    print("Warning: skipping run with missing spots/bases, run ", run['accn'], " sample", sampleAccn, ":", run)
                else:
                    print("Run accn:", run['accn'])
                    cursor.execute('INSERT INTO run (experiment_id,accn,total_spots,total_bases) VALUES (%s,%s,%s,%s) RETURNING experiment_id',
                                   [experimentId, run['accn'], run['spots'], run['bases']])

            db.commit()


def main(args=None):
    if 'accn' in args: # for debug
        if ('key' in args) and ('email' in args):
            Entrez.api_key = args['key']
            Entrez.email = args['email']
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
    parser.add_argument('-k', '--key')   # For NCBI Entrez calls
    parser.add_argument('-e', '--email') # For NCBI Entrez calls
    parser.add_argument('-a', '--accn')  # optional single accn to load (for debugging)

    main(args={k: v for k, v in vars(parser.parse_args()).items() if v})

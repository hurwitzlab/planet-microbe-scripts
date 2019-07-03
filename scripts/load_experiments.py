#!/usr/bin/env python3
import sys
from Bio import Entrez
# from Bio.Blast import NCBIXML
import xml.etree.ElementTree as ET


Entrez.api_key = sys.argv[1]
Entrez.email = sys.argv[2]

accn = "SAMEA3512136" # test

handle = Entrez.esearch(db='biosample', term=accn)
result = Entrez.read(handle)

if result['Count'] == 0:
    raise Exception("BioSample accn not found:", accn)

idList = result['IdList']
for id in idList:
    handle = Entrez.esummary(db='biosample', id=id, retmode='xml')
    result = Entrez.read(handle)
    for summary in result['DocumentSummarySet']['DocumentSummary']:
        # NCBIXML raises error "AttributeError: 'StringElement' object has no attribute 'read'"
        # for record in NCBIXML.read(summary['SampleData']):
        #     print(record)

        record = ET.fromstring(summary['SampleData'])
        attr = record.find(".//Attribute[@attribute_name='SRA accession']")
        if attr == None:
            raise Exception("Could not parse SRA accn for BioSample:", accn)
        sraAccn = attr.text
        print("sra accession:", sraAccn)

        handle = Entrez.esearch(db='sra', term=sraAccn)
        result = Entrez.read(handle)
        idList = result['IdList']
        for id in idList:
            handle = Entrez.esummary(db='sra', id=id, retmode='xml')
            result = Entrez.read(handle)
            for record in result:
                doc = ET.fromstring('<root>' + record['ExpXml'] + '</root>')
                exp = doc.find(".//Experiment")
                name = exp.attrib['name']
                accn = exp.attrib['acc']
                print("experiment accn:", accn)
                print("experiment name:", name)


                doc = ET.fromstring('<root>' + record['Runs'] + '</root>')
                run = doc.find(".//Run")
                accn = run.attrib['acc']
                totalSpots = run.attrib['total_spots']
                totalBases = run.attrib['total_bases']
                print("run accn:", accn)
                print("total spots:", totalSpots)
                print("total bases:", totalBases)

handle.close()

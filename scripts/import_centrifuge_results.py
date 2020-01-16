#!/usr/bin/env python3
import sys
import os
import argparse
import subprocess
import psycopg2
import csv


def ils(path):
    return subprocess.check_output(['ils', path]).decode('UTF-8').split('\n')


def iget(srcPath):
    try:
        subprocess.run(["iget", "-Tf", srcPath])
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


def fetch_run_id(db, accn):
    cursor = db.cursor()
    cursor.execute('SELECT run_id FROM run WHERE accn=%s', [accn])
    if cursor.rowcount == 0:
        return None
    row = cursor.fetchone()
    return row[0]


def insert_taxonomy(db, taxId, name):
    cursor = db.cursor()
    cursor.execute("INSERT INTO taxonomy (tax_id,name) VALUES (%s,%s) ON CONFLICT(tax_id) DO UPDATE SET tax_id=EXCLUDED.tax_id RETURNING tax_id", [taxId, name])
    return cursor.fetchone()[0]


def insert_centrifuge(db, runId, taxId, numReads, numUniqueReads, abundance):
    cursor = db.cursor()
    cursor.execute("INSERT INTO centrifuge (run_id,tax_id,num_reads,num_unique_reads,abundance) VALUES (%s,%s,%s,%s,%s) RETURNING centrifuge_id", [runId, taxId, numReads, numUniqueReads, abundance])
    return cursor.fetchone()[0]


def import_centrifuge(db, accn, filepath):
    runId = fetch_run_id(db, accn)
    if not runId:
        return # Run not in DB

    print("Importing file", filepath)
    iget(filepath)

    filename = os.path.basename(filepath)
    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter="\t")
        next(reader, None)  # skip header line
        for i, line in enumerate(reader):
            taxId = insert_taxonomy(db, line[1], line[0])
            insert_centrifuge(db, runId, taxId, line[4], line[5], line[6])
        db.commit()
    os.remove(filename)


def main(args=None):
    conn = psycopg2.connect(host='', dbname=args['dbname'], user=args['username'], password=args['password'] if 'password' in args else None)

    listing = ils(args['inputdir'])
    for filename in listing:
        filename = filename.strip()
        if not filename.endswith('.tsv'):
            continue

        accn = filename.split('.')[0]
        if 'accn' in args and args['accn'] != accn: # for debug
            continue

        import_centrifuge(conn, accn, args['inputdir'] + '/' + filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import Centrifuge results into database')
    parser.add_argument('-d', '--dbname', required=True)
    parser.add_argument('-u', '--username', required=True)
    parser.add_argument('-p', '--password', required=False, default='')
    parser.add_argument('-i', '--inputdir', default='/iplant/home/shared/planetmicrobe/centrifuge')    # path to centrifuge results in Data Store
    parser.add_argument('-a', '--accn')  # optional: run accn to load (for debugging)

    main(args={k: v for k, v in vars(parser.parse_args()).items() if v})

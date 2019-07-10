#!/usr/bin/env python3
import sys
import argparse
import subprocess
import psycopg2


def get_runs(db):
    cursor = db.cursor()
    cursor.execute('SELECT accn FROM run')
    return list(map(lambda row: row[0], cursor.fetchall()))


def import_sra_data(accn):
    rc = subprocess.run(["fastq-dump", "--split-files", "--fasta", "--gzip", "--accession", accn])
    if rc != 0:
        raise Exception("fastq-dump returned", rc)

    ls = subprocess.check_output(["ls", accn + "*.fasta.gz"])
    print(ls)


def main(args=None):
    if 'accn' in args: # for debug
        print("foo")
    else: # load all experiments and runs into db
        if 'password' in args:
            conn = psycopg2.connect(host='', dbname=args['dbname'], user=args['username'], password=args['password'])
        else:
            conn = psycopg2.connect(host='', dbname=args['dbname'], user=args['username'])

    accnList = get_runs(conn)
    print(accnList)
    for accn in accnList:
        import_sra_data(accn)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load datapackage into database.')
    parser.add_argument('-d', '--dbname')
    parser.add_argument('-u', '--username')
    parser.add_argument('-p', '--password')
    parser.add_argument('-a', '--accn')

    main(args={k: v for k, v in vars(parser.parse_args()).items() if v})

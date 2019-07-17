#!/usr/bin/env python3
import sys
import os
import glob
import argparse
import subprocess
import psycopg2


def get_runs(db):
    cursor = db.cursor()
    cursor.execute('SELECT accn FROM run')
    return list(map(lambda row: row[0], cursor.fetchall()))


def fastq_dump(accn, stagingdir):
    print("Downloading", accn)

    try:
        subprocess.run(["fastq-dump", "--split-files", "--fasta", "--gzip", "--accession", accn, "--outdir", stagingdir])
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

    return glob.glob(stagingdir + "/" + accn + "*.fasta.gz")


def iput(srcPath, destPath):
    print("Transferring to IRODS", srcPath, destPath)
    try:
        subprocess.run(["iput", "-Tf", srcPath, destPath])
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


def fetch_run_id(db, accn):
    cursor = db.cursor()
    cursor.execute('SELECT run_id FROM run WHERE accn=%s', [accn])
    row = cursor.fetchone()
    return row[0]


def import_data(db, accn, stagingdir, targetdir, skipIput):
    cursor = db.cursor()

    fileList = sorted(fastq_dump(accn, stagingdir))
    print("files:", fileList)

    fileTypeId = insert_file_type(db, 'sequence')
    fileFormatId = insert_file_format(db, 'fasta')

    for f in fileList:
        if not skipIput:
            iput(f, targetdir)

        runId = fetch_run_id(db, accn)
        irodsPath = targetdir + "/" + os.path.basename(f)
        cursor.execute('INSERT INTO file (file_type_id,file_format_id,url) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING RETURNING file_id',
                       [fileTypeId, fileFormatId, irodsPath])
        fileId = cursor.fetchone()[0]
        cursor.execute(
            'INSERT INTO run_to_file (run_id,file_id) VALUES (%s,%s) ON CONFLICT DO NOTHING',
            [runId, fileId]
        )

        os.remove(f)

    db.commit()


def main(args=None):
    if 'password' in args:
        conn = psycopg2.connect(host='', dbname=args['dbname'], user=args['username'], password=args['password'])
    else:
        conn = psycopg2.connect(host='', dbname=args['dbname'], user=args['username'])

    if 'accn' in args: # for debug
        import_data(conn, args['accn'], args['stagingdir'], args['targetdir'])
    else: # load all experiments and runs into db
        accnList = get_runs(conn)
        print("accn:", accnList)
        for accn in accnList:
            import_data(conn, accn, args['stagingdir'], args['targetdir'], 'skipirods' in args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load datapackage into database.')
    parser.add_argument('-d', '--dbname')
    parser.add_argument('-u', '--username')
    parser.add_argument('-p', '--password')
    parser.add_argument('-s', '--stagingdir')
    parser.add_argument('-t', '--targetdir')
    parser.add_argument('-a', '--accn')
    parser.add_argument('-x', '--skipirods', action='store_true')

    main(args={k: v for k, v in vars(parser.parse_args()).items() if v})

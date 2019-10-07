#!/usr/bin/env python3
import sys
import os
import glob
import argparse
import subprocess
import psycopg2
import json


def fastq_dump(accn, stagingdir):
    print("Downloading", accn)

    try:
        # subprocess.run(["fastq-dump", "--split-files", "--fasta", "--gzip", "--accession", accn, "--outdir", stagingdir])
        subprocess.run(["fasterq-dump", "--split-3", "--progress", "--temp", stagingdir, "--outdir", stagingdir, accn])
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

    return glob.glob(stagingdir + "/" + accn + "*.fastq")


def gzip(path):
    print("Compressing", path)

    try:
        subprocess.run(["gzip", path])
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

    return path + ".gz"


def iput(srcPath, destPath):
    print("Transferring to IRODS", srcPath, destPath)
    try:
        subprocess.run(["iput", "-Tf", srcPath, destPath])
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


def ils(path):
    return subprocess.check_output(['ils', path]).decode('UTF-8').split('\n')


def fetch_runs(db, projectId):
    cursor = db.cursor()

    if projectId == None:
        cursor.execute('SELECT accn FROM run')
    else:
        cursor.execute('SELECT r.accn FROM run r JOIN experiment e ON e.experiment_id=r.experiment_id JOIN sample s ON s.sample_id=e.sample_id JOIN project_to_sample pts ON pts.sample_id=s.sample_id WHERE pts.project_id=%s', [projectId])

    return list(map(lambda row: row[0], cursor.fetchall()))


def fetch_run_id(db, accn):
    cursor = db.cursor()
    cursor.execute('SELECT run_id FROM run WHERE accn=%s', [accn])
    row = cursor.fetchone()
    return row[0]


def insert_file_type(db, name):
    cursor = db.cursor()
    cursor.execute("INSERT INTO file_type (name) VALUES (%s) ON CONFLICT(name) DO UPDATE SET name=EXCLUDED.name RETURNING file_type_id", [name])
    return cursor.fetchone()[0]


def insert_file_format(db, name):
    cursor = db.cursor()
    cursor.execute("INSERT INTO file_format (name) VALUES (%s) ON CONFLICT(name) DO UPDATE SET name=EXCLUDED.name RETURNING file_format_id", [name])
    return cursor.fetchone()[0]


def import_data(db, accn, args, listing):
    stagingdir = args['stagingdir']
    targetdir = args['targetdir']
    skipIput = 'skipirods' in args
    skipDB = 'skipdb' in args

    exists = []
    for line in listing:
        line = line.strip()
        if line.find(accn + '_') >= 0 or line.find(accn + '.') >= 0:
            exists.append(line)

    if len(exists) > 0:
        print("Found previously imported files", exists)
        if not skipDB:
            for f in exists:
                irodsPath = targetdir + "/" + f
                insert_file(db, accn, irodsPath)
    else:
        fileList = sorted(fastq_dump(accn, stagingdir))
        print("files:", fileList)

        for f in fileList:
            f = gzip(f)

            if not skipIput:
                iput(f, targetdir)

            irodsPath = targetdir + "/" + os.path.basename(f)
            if not skipDB:
                insert_file(db, accn, irodsPath)

            os.remove(f)


def insert_file(db, accn, irodsPath):
    cursor = db.cursor()

    fileTypeId = insert_file_type(db, 'sequence')
    fileFormatId = insert_file_format(db, 'fastq')

    runId = fetch_run_id(db, accn)

    cursor.execute(
        'INSERT INTO file (file_type_id,file_format_id,url) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING RETURNING file_id',
        [fileTypeId, fileFormatId, irodsPath])
    fileId = cursor.fetchone()[0]

    cursor.execute(
        'INSERT INTO run_to_file (run_id,file_id) VALUES (%s,%s) ON CONFLICT DO NOTHING',
        [runId, fileId]
    )

    db.commit()


def main(args=None):
    conn = None
    if not 'skipdb' in args:
        conn = psycopg2.connect(host='', dbname=args['dbname'], user=args['username'], password=args['password'] if 'password' in args else None)

    listing = ils(args['targetdir'])

    if 'accn' in args: # for debug
        import_data(conn, accn, args, listing)
    else: # load all experiments and runs into db
        if 'accnfile' in args:
            with open(args['accnfile'], 'r') as data_file:
                json_data = data_file.read()
            accnList = json.loads(json_data, strict=False)
        else:
            accnList = fetch_runs(conn, args['projectId'] if 'projectId' in args else None)

        print("accn:", accnList)
        print(len(accnList), "runs")
        for accn in accnList:
            import_data(conn, accn, args, listing)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load datapackage into database.')
    parser.add_argument('-d', '--dbname')
    parser.add_argument('-u', '--username')
    parser.add_argument('-p', '--password')
    parser.add_argument('-s', '--stagingdir')  # temporary staging path
    parser.add_argument('-t', '--targetdir')   # target path in Data Store
    parser.add_argument('-pid', '--projectId') # optional project ID
    parser.add_argument('-a', '--accn')        # optional single accn to load (for debugging)
    parser.add_argument('-f', '--accnfile')    # optional file containing a JSON list of accn's to load (for debugging)
    parser.add_argument('-x', '--skipirods', action='store_true') # don't copy files to Data Store
    parser.add_argument('-y', '--skipdb', action='store_true')    # don't load files into DB

    main(args={k: v for k, v in vars(parser.parse_args()).items() if v})

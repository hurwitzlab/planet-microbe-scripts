#!/usr/bin/env python3
"""
Delete a project in the DB
"""

import sys
import argparse
import subprocess
import psycopg2


def delete_all(db):
    print("Deleting all tables ...")
    cursor = db.cursor()
    cursor.execute("DELETE FROM project_to_sample; DELETE FROM sample_to_sampling_event; DELETE FROM project_to_file; DELETE FROM run_to_file; DELETE FROM file; DELETE FROM file_type; DELETE FROM file_format; DELETE FROM run; DELETE FROM library; DELETE FROM experiment; DELETE FROM sample; DELETE FROM project; DELETE FROM schema; DELETE FROM sampling_event; DELETE FROM campaign;")
    db.commit()


def delete_project(db, projectId, irodsPath):
    print("Deleting project", projectId)
    cursor = db.cursor()

    cursor.execute("SELECT sample_id FROM project_to_sample WHERE project_id=%s", (projectId,))
    for row in cursor.fetchall():
        sampleId = row[0]

        cursor.execute("SELECT experiment_id FROM experiment WHERE sample_id=%s", (sampleId,))
        for row2 in cursor.fetchall():
            experimentId = row2[0]
            cursor.execute("DELETE FROM library WHERE experiment_id=%s", (experimentId,))

            cursor.execute("SELECT run_id FROM run WHERE experiment_id=%s", (experimentId,))
            for row3 in cursor.fetchall():
                runId = row3[0]
                cursor.execute("DELETE FROM run_to_file WHERE run_id=%s", (runId,))
                cursor.execute("DELETE FROM run WHERE run_id=%s", (runId,))

            cursor.execute("DELETE FROM experiment WHERE experiment_id=%s", (experimentId,))

        cursor.execute('DELETE FROM project_to_sample WHERE project_id=%s AND sample_id=%s', (projectId, sampleId))
        cursor.execute("DELETE FROM sample_to_sampling_event WHERE sample_id=%s", (sampleId,))
        cursor.execute("DELETE FROM sample WHERE sample_id=%s", (sampleId,))

    cursor.execute("SELECT file_id FROM project_to_file WHERE project_id=%s", (projectId,))
    for row in cursor.fetchall():
        fileId = row[0]
        cursor.execute("DELETE FROM project_to_file WHERE project_id=%s AND file_id=%s", (projectId,fileId))
        cursor.execute("DELETE FROM file WHERE file_id=%s", (fileId,))
    cursor.execute("DELETE FROM project WHERE project_id=%s", (projectId,))

    cursor.execute("""
        SELECT se.sampling_event_id,stse.sample_to_sampling_event_id 
        FROM sampling_event se
        LEFT JOIN sample_to_sampling_event stse ON stse.sampling_event_id=se.sampling_event_id;
        """)
    for row in cursor.fetchall():
        if row[1] == None:
            cursor.execute("DELETE FROM sampling_event WHERE sampling_event_id=%s", (row[0],))

    cursor.execute("""
        SELECT c.campaign_id,se.sampling_event_id 
        FROM campaign c
        LEFT JOIN sampling_event se ON se.campaign_id=c.campaign_id;
        """)
    for row in cursor.fetchall():
        if row[1] == None:
            cursor.execute("DELETE FROM campaign WHERE campaign_id=%s", (row[0],))

    db.commit()

    if irodsPath:
        irmdir(irodsPath + '/' + projectId)


def irmdir(path):
    print("Removing from IRODS", path)
    try:
        subprocess.run(["irm", "-r", path])
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


def get_project_by_accn(db, accn):
    cursor = db.cursor()
    cursor.execute("SELECT project_id FROM project WHERE accn=%s", (accn,))
    return cursor.fetchone()[0]


def main(args=None):
    if 'password' in args:
        conn = psycopg2.connect(host='', dbname=args['dbname'], user=args['username'], password=args['password'])
    else:
        conn = psycopg2.connect(host='', dbname=args['dbname'], user=args['username'])

    irodsPath = args['irodspath'] if 'irodspath' in args else None

    if 'deleteall' in args:
        delete_all(conn)
    elif 'projectId' in args:
        delete_project(conn, args['projectId'], irodsPath)
    elif 'accn' in args:
        id = get_project_by_accn(conn, args['accn'])
        delete_project(conn, id, irodsPath)
    else:
        print("Specify -x, -i, or -n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load datapackage into database.')
    parser.add_argument('-d', '--dbname')
    parser.add_argument('-u', '--username')
    parser.add_argument('-p', '--password')
    parser.add_argument('-x', '--deleteall', action='store_true')
    parser.add_argument('-pid', '--projectId')
    parser.add_argument('-i', '--irodspath')  # optional IRODS path to store CTD and Niskin files
    parser.add_argument('-n', '--accn')

    main(args={k: v for k, v in vars(parser.parse_args()).items() if v})

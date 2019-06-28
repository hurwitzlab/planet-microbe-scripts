#!/usr/bin/env python3
"""
Delete project

delete_project.py -d <database> -u <username> -p <password> <project_id>
"""

import sys
import argparse
import psycopg2


def delete_all(db):
    print("Deleting all tables ...")
    cursor = db.cursor()
    cursor.execute("DELETE FROM project_to_sample; DELETE FROM sample_to_sampling_event; DELETE FROM sample; DELETE FROM project; DELETE FROM schema; DELETE FROM sampling_event; DELETE FROM campaign;")
    db.commit()


def delete_project(db, projectId):
    print("Deleting project", projectId)
    cursor = db.cursor()

    cursor.execute("SELECT sample_id FROM project_to_sample WHERE project_id=%s", (projectId,))
    for row in cursor.fetchall():
        sampleId = row[0]
        cursor.execute('DELETE FROM project_to_sample WHERE project_id=%s AND sample_id=%s', (projectId, sampleId))
        cursor.execute("DELETE FROM sample_to_sampling_event WHERE sample_id=%s", (sampleId,))
        cursor.execute("DELETE FROM sample WHERE sample_id=%s", (sampleId,))

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


def get_project_by_accn(db, accn):
    cursor = db.cursor()

    cursor.execute("SELECT project_id FROM project WHERE accn=%s", (accn,))
    return cursor.fetchone()[0]


def main(args=None):
    if 'password' in args:
        conn = psycopg2.connect(host='', dbname=args['dbname'], user=args['username'], password=args['password'])
    else:
        conn = psycopg2.connect(host='', dbname=args['dbname'], user=args['username'])

    if 'deleteall' in args:
        delete_all(conn)
    elif 'id' in args:
        delete_project(conn, args['id'])
    elif 'accn' in args:
        id = get_project_by_accn(conn, args['accn'])
        delete_project(conn, id)
    else:
        print("Specify -x, -i, or -n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load datapackage into database.')
    parser.add_argument('-d', '--dbname')
    parser.add_argument('-u', '--username')
    parser.add_argument('-p', '--password')
    parser.add_argument('-x', '--deleteall', action='store_true')
    parser.add_argument('-i', '--id')
    parser.add_argument('-n', '--accn')

    main(args={k: v for k, v in vars(parser.parse_args()).items() if v})

#!/usr/bin/python3

import argparse
import hashlib
import os, sys
import sqlite3
from concurrent import futures
from queue import Queue
from sqlite3 import Error

db_path=''
max_thread_count = 10
file_queue = Queue()
result_queue = Queue()

def sha256sum(filepath):
    h = hashlib.sha256()
    b = bytearray(128*1024)
    mv = memoryview(b)
    with open(filepath, 'rb', buffering=0) as file:
        for n in iter(lambda : file.readinto(mv), 0):
            h.update(mv[:n])
    return {'filepath' : filepath, 'sha256sum' : h.hexdigest()}

def check_files():
    items = set(file_queue.queue)
    with futures.ThreadPoolExecutor(max_thread_count) as tpe:
        tasks = [tpe.submit(sha256sum, item) for item in items]
        for future in futures.as_completed(tasks):
            yield future.result()

def create_connection():
    conn = None
    try:
        conn = sqlite3.connect(db_path)
    except Error as e:
        print('Error occurred: {e}'.format(e=e))
    return conn

def db_create():
    conn = create_connection()
    curs = conn.cursor()
    curs.execute('DROP TABLE IF EXISTS files')
    curs.execute('CREATE TABLE files (id INTEGER PRIMARY KEY AUTOINCREMENT, filepath VARCHAR(256), sha256sum VARCHAR(64))')
    ps = 'INSERT INTO files (filepath, sha256sum) VALUES(?, ?)'

    for element in list(result_queue.queue):
        curs.execute(ps, (element['filepath'], element['sha256sum']))
    curs.execute('select count(id) from files')
    number = curs.fetchall()[0]
    curs.execute('select count(sha256sum) from files group by sha256sum having ( count(sha256sum) > 1 )')
    duplicates = curs.fetchone()[0]
    curs.close(), conn.commit(), conn.close()

    if args.list or args.temp:
        print('Scanned [{number}] files for duplicates.'.format(count=duplicates, number=number))
        db_query()
    else:
        print('Scanned [{number}] files for duplicates, use [-l] to list results'.format(count=duplicates, number=number))

def db_query(db_file=str):
    results = list()
    ps = 'SELECT filepath FROM files WHERE sha256sum=?'
    conn = create_connection()
    curs = conn.cursor()
    curs.execute('select sha256sum, count(*) from files group by sha256sum having ( count(sha256sum) > 1 )')
    for row in curs.fetchall():
        results.append([row[0],row[1]])
    for r in results:
        print('-----------------------------------------------------------------')
        print('The file with hash [{h}] has [{n}] occurrances:'.format(h=r[0],n=r[1]))
        for i in curs.execute(ps, (r[0],)).fetchall():
            print(i)
    curs.close(), conn.close()

def check_path(path):
    if not os.path.isdir(path) or not os.access(path, os.X_OK):
      print('The path specified does not exist or the user does not have sufficient access rights')
      print(path)
      sys.exit(1)
    for root, _, files in os.walk(path):
        for file in files:
            file_queue.put(os.path.join(root, file))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Check for duplicates in the specified dir, results are stored in sqlite3 database')
    parser.add_argument('Path', metavar='path', type=str, help='Path to perform the actions')
    parser.add_argument('-f', '--force', action='store_true' ,help='Drop database if exists and perform a re-scan')
    parser.add_argument('-l', '--list', action='store_true', help='Show the results (will perform scan if no db is found)')
    parser.add_argument('-t', '--temp', action='store_true', help='Use a temporary db and remove it after.')
    args = parser.parse_args()
    check_path(args.Path)

    if args.temp or args.force or (args.list and not os.path.isfile(db_path)):
        for result in check_files():
            result_queue.put(result)
        if args.temp:
            import time
            db_path = '/tmp/{t}.db'.format(t=int(time.time()))
        else:
            db_path = '{path}/duplicates.db'.format(path=args.Path)
        db_create()
    elif args.list and os.path.isfile(db_path):
        db_query()
    else:
        parser.print_help()


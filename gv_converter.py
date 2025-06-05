from types import NoneType
from pypxlib.pxlib_ctypes import *
from pypxlib import Table
import sqlite3
from sqlite3 import Error
import sys, getopt
import argparse

from parser_sql import convert_db
import os

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)


def main(argv):
    argParser = argparse.ArgumentParser()
    # codecs: https://docs.python.org/3/library/codecs.html
    argParser.add_argument("-c", "--codec", help="Source Paradox table CODE PAGE")
    argParser.add_argument("-i", "--in-files", nargs='+', help="Source Paradox table files")
    argParser.add_argument("-o", "--out-file", help="Destination Sqlite DB file")
    argParser.add_argument("-p", "--path", help="Path to source Paradox table files (Optional)", default="")

    args = argParser.parse_args()

    print("args=%s" % args)
    print("args.in-files=%s" % args.in_files)
    print("args.codecs=%s" % args.codec)

    conn = create_connection(args.out_file)

    for file_db in args.in_files:
        if args.path:
            file_db = os.path.join(args.path, file_db)
        convert_db(conn, file_db, args.codec)
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
   main(sys.argv[1:])

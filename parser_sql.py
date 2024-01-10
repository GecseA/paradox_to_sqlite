import codecs
import datetime
from types import NoneType
from pypxlib.pxlib_ctypes import *
from pypxlib import Table
import sqlite3
from sqlite3 import Error
import sys, getopt
import os

def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def insert_record(conn, insert_sql):
    try:
        c = conn.cursor()
        c.execute(insert_sql)
        conn.commit()
        return c.lastrowid
    except Error as e:
        print(e)


def convert_db(conn, source_file: str, codec):

    # filename -> Sqlite Table name
    base_dir_pair = os.path.split(source_file)
    #print(base_dir_pair)
    root_ext_pair = os.path.splitext(base_dir_pair[1])
    filename = root_ext_pair[0]
    file_ext = root_ext_pair[1]
    # ('./dir/subdir/filename', '.ext')
    #print(type(root_ext_pair))

    print(source_file)
    print(filename)

    # open Paradox DB file
    pxdoc = PX_new()
    PX_open_file(pxdoc, source_file.encode())

    filename = filename.lower()

    num_fields = PX_get_num_fields(pxdoc)
    print('db has %d fields:' % num_fields)

    source_table = " CREATE TABLE IF NOT EXISTS " + filename + " ("
    insert_seq = " INSERT INTO " + filename +" ("
    fields = list()

    # iterate over tale fields
    for i in range(num_fields):
        field = PX_get_field(pxdoc, i)
        
        fieldname = str(field.contents.px_fname).replace("b'", "").strip("'")
        fields.append(fieldname)
        
        # convert Paradox Field types to Sqlite 
        if (int.from_bytes(field.contents.px_ftype, "big") == pxfAlpha) :
            source_table += fieldname + " text,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfDate) : 
            source_table += fieldname + " text,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfShort) : 
            source_table += fieldname + " integer,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfLong) : 
            source_table += fieldname + " integer,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfCurrency) : 
            source_table += fieldname + " pxfCurrency"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfNumber) : 
            source_table += fieldname + " integer,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfLogical) : 
            source_table += fieldname + " pxfLogical"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfMemoBLOb) : 
            source_table += fieldname + " pxfMemoBLOb"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfBLOb) : 
            source_table += fieldname + " pxfBLOb"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfFmtMemoBLOb) : 
            source_table += fieldname + " pxfFmtMemoBLOb"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfOLE) : 
            source_table += fieldname + " pxfOLE"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfGraphic) : 
            source_table += fieldname + " pxfGraphic"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfTime) : 
            source_table += fieldname + " pxfTime"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfTimestamp) : 
            source_table += fieldname + " pxfTimestamp"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfAutoInc) : 
            source_table += fieldname + " integer PRIMARY KEY,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfBCD) : 
            source_table += fieldname + " pxfBCD"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfBytes) : 
            source_table += fieldname + " pxfBytes"
        else :
            source_table += fieldname + " UNKNOWN"

        insert_seq += fieldname + ","

    # cut the last ','
    source_table = source_table[:len(source_table)-1]
    source_table += ")"
    insert_seq = insert_seq[:len(insert_seq)-1]
    insert_seq += ") VALUES ("

    # Sqlite create statement
    print(source_table)

    # Close the file:
    PX_close(pxdoc)
    # Free the memory associated with pxdoc:
    PX_delete(pxdoc)

    # migrate from Pdox to Sqlite
    table = Table(source_file)
    if conn is not None:
        create_table(conn, source_table)
    else:
        print("Error! cannot create the database connection.")
        
    for row in table:
        insert_query = insert_seq
        for f in fields:
            #print(row[f])
            if(type(row[f]) == int):
                insert_query += str(row[f]) + ","
            elif(type(row[f]) == str):
                
                try:
                    # !!! CHECK CODE PAGE !!! ex.: cp852 for HU
                    inputText = str.encode(row[f], codec, "ignore")
                    converted = str(inputText.decode('iso-8859-2'))
                    #print(converted)
                    insert_query += "'" + converted + "',"
                except Error as e:
                    insert_query += "'" + str(row[f]) + "',"

            elif(type(row[f]) == datetime.date):
                insert_query += "'" + str(row[f]) + "',"
            elif(type(row[f]) == NoneType):
                insert_query += "null,"
            else:
                print(type(row[f]))
        
        insert_query = insert_query[:len(insert_query)-1]
        insert_query += "); "   
        #print(insert_query)
        lastid = insert_record(conn, insert_query)








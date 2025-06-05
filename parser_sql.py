import codecs
import datetime
from types import NoneType
from pypxlib.pxlib_ctypes import *
from pypxlib import Table
from sqlite3 import Error
import os
import numbers

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
    
    if file_ext.lower() == "":
        source_file += ".db"

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

    # iterate over table fields
    for i in range(num_fields):
        field = PX_get_field(pxdoc, i)
        fieldname = str(field.contents.px_fname).replace("b'", "").strip("'")
        fields.append(fieldname)
        output_field_name = fieldname.replace(" ", "_")
        
        # convert Paradox Field types to Sqlite 
        source_table += output_field_name + " "
        if (int.from_bytes(field.contents.px_ftype, "big") == pxfAlpha) :
            source_table += "TEXT,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfDate) : 
            source_table += "TEXT,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfShort) : 
            source_table += "INTEGER,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfLong) : 
            source_table += "INTEGER,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfCurrency) : 
            source_table += "NUMERIC,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfNumber) : 
            source_table += "INTEGER,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfLogical) : 
            source_table += "BOOL,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfMemoBLOb) : 
            source_table += "TEXT,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfBLOb) : 
            source_table += "pxfBLOb,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfFmtMemoBLOb) : 
            source_table += "pxfFmtMemoBLOb,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfOLE) : 
            source_table += "pxfOLE,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfGraphic) : 
            source_table += "pxfGraphic,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfTime) : 
            source_table += "TIME,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfTimestamp) : 
            source_table += "DATETIME,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfAutoInc) : 
            source_table += "INTEGER PRIMARY KEY,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfBCD) : 
            source_table += "pxfBCD,"
        elif (int.from_bytes(field.contents.px_ftype, "big") == pxfBytes) : 
            source_table += "pxfBytes,"
        else :
            source_table += "UNKNOWN,"

        insert_seq += output_field_name + ","

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
    
    input_count = 0
    output_count = 0    
    for row in table:
        input_count += 1
        insert_query = insert_seq
        skipInsert = False
        for f in fields:
            if isinstance(row[f], int) or isinstance(row[f], numbers.Number) or isinstance(row[f], float):
                insert_query += str(row[f]) + ","
            elif isinstance(row[f], str):                
                try:
                    # !!! CHECK CODE PAGE !!! ex.: cp852 for HU
                    inputText = str.encode(row[f], codec, "ignore")
                    converted = str(inputText.decode('iso-8859-2'))
                    #print(converted)
                    safe_converted = converted.replace("'", "''")
                    insert_query += "'" + safe_converted + "',"
                except Error as e:
                    insert_query += "'" + str(row[f]) + "',"

            elif isinstance(row[f], datetime.date) or isinstance(row[f], datetime.datetime)or isinstance(row[f], datetime.time):
                insert_query += "'" + str(row[f]) + "',"
            elif row[f] is None:
                insert_query += "null,"
            elif isinstance(row[f], bool):
                insert_query += str(row[f]) + ","
            else:
                print("Field %s in record %d is of type %s" % (f, input_count, type(row[f])))
                skipInsert = True
        
        if skipInsert:
            continue
        
        insert_query = insert_query[:len(insert_query)-1]
        insert_query += "); "   
        #print(insert_query)
        try:
            insert_record(conn, insert_query)
            output_count += 1
        except Exception as e:
            print("Error inserting record:", e)
            raise e
        
    print("%d records read from %s, %d records added to Sqlite table %s" % (input_count, source_file, output_count, filename))
    table.close()
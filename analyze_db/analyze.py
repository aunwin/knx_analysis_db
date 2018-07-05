#!/usr/bin/env python3

import mysql.connector
from mysql.connector import errorcode
from timeit import default_timer as timer
from config import databaseconfig as db_cfg


def analyze(query, database_connection, database_cursor):
    start = timer()

    database_cursor.execute(query)

    for row in database_cursor:
        print(row)

    end = timer()
    runtime = end - start
    print(f'Program was running for {runtime:.4} seconds')
    return


def init_db_connections():
    database_connection = None
    database_cursor = None

    try:
        database_connection = mysql.connector.connect(**db_cfg.src_db)
        database_cursor = database_connection.cursor()

        # get version of database
        database_cursor.execute("SELECT VERSION()")
        db_version = database_cursor.fetchone()
        print(f'DB version: {db_version}')

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)

    return database_connection,database_cursor


def close_db_connection(database_connection, database_cursor):
    # Clean up
    database_cursor.close()
    database_connection.close()

    return


db_conn, db_csr = init_db_connections()
analyze("SELECT * FROM bus_dump.knx_dump_test LIMIT 10;", db_conn, db_csr)
close_db_connection(db_conn, db_csr)

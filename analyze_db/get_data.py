#!/usr/bin/env python3

import mysql.connector
import csv
from mysql.connector import errorcode
from timeit import default_timer as timer
from config import databaseconfig as db_cfg


def get_data(database_cursor):
    get_different_source_addresses = False
    get_number_of_apdus_per_physical_address = False
    get_data_of_weather_station = False
    get_different_apcis = False
    get_different_tpcis = False

    if get_different_source_addresses:
        start = timer()
        query = "SELECT DISTINCT source_addr FROM bus_dump.knx_dump_test;"
        database_cursor.execute(query)
        column_names = [i[0] for i in database_cursor.description]
        with open('data/get_different_source_addresses.csv', 'w') as file:
            csv_out = csv.writer(file, lineterminator='\n', delimiter='\t')
            csv_out.writerow(column_names)
            for row in database_cursor:
                csv_out.writerow(row)
        file.close()

        end = timer()
        runtime = end - start
        print(f'Query ({query})was running for {runtime:.4} seconds')

    if get_number_of_apdus_per_physical_address:
        start = timer()
        query = "SELECT DISTINCT source_addr AS src, (SELECT COUNT(DISTINCT apdu) FROM bus_dump.knx_dump_test WHERE source_addr=src) FROM bus_dump.knx_dump_test;"
        database_cursor.execute(query)
        column_names = [i[0] for i in database_cursor.description]
        with open('data/get_number_of_apdus_per_physical_address.csv', 'w') as file:
            csv_out = csv.writer(file, lineterminator='\n', delimiter='\t')
            csv_out.writerow(column_names)
            for row in database_cursor:
                csv_out.writerow(row)
        file.close()

        end = timer()
        runtime = end - start
        print(f'Query ({query})was running for {runtime:.4} seconds')

    if get_data_of_weather_station:
        start = timer()
        query = "SELECT * FROM bus_dump.knx_dump_test WHERE source_addr = '3.3.29';"
        database_cursor.execute(query)
        column_names = [i[0] for i in database_cursor.description]
        with open('data/get_data_of_weather_station.csv', 'w') as file:
            csv_out = csv.writer(file, lineterminator='\n', delimiter='\t')
            csv_out.writerow(column_names)
            for row in database_cursor:
                csv_out.writerow(row)
        file.close()

        end = timer()
        runtime = end - start
        print(f'Query ({query})was running for {runtime:.4} seconds')

    if get_different_apcis:
        start = timer()
        query = "SELECT DISTINCT apci AS value, (SELECT COUNT(apci) FROM bus_dump.knx_dump_test WHERE apci=value) FROM bus_dump.knx_dump_test;"
        database_cursor.execute(query)
        column_names = [i[0] for i in database_cursor.description]
        with open('data/get_different_apcis.csv', 'w') as file:
            csv_out = csv.writer(file, lineterminator='\n', delimiter='\t')
            csv_out.writerow(column_names)
            for row in database_cursor:
                csv_out.writerow(row)
        file.close()

        end = timer()
        runtime = end - start
        print(f'Query ({query})was running for {runtime:.4} seconds')

    if get_different_tpcis:
        start = timer()
        query = "SELECT DISTINCT tpci AS value, (SELECT COUNT(tpci) FROM bus_dump.knx_dump_test WHERE tpci=value) FROM bus_dump.knx_dump_test;"
        database_cursor.execute(query)
        column_names = [i[0] for i in database_cursor.description]
        with open('data/get_different_tpcis.csv', 'w') as file:
            csv_out = csv.writer(file, lineterminator='\n', delimiter='\t')
            csv_out.writerow(column_names)
            for row in database_cursor:
                csv_out.writerow(row)
        file.close()

        end = timer()
        runtime = end - start
        print(f'Query ({query})was running for {runtime:.4} seconds')

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

    return database_connection, database_cursor


def close_db_connection(database_connection, database_cursor):
    # Clean up
    database_cursor.close()
    database_connection.close()

    return


db_conn, db_csr = init_db_connections()
get_data(db_csr)
close_db_connection(db_conn, db_csr)

#!/usr/bin/env python3

import mysql.connector
from mysql.connector import errorcode
from timeit import default_timer as timer
from migrate_db import srcRow
from migrate_db import sinkRow
from config import databaseconfig as db_cfg

import baos_knx_parser as knx


def migrate_records(offset, row_cnt, workload_size, read_cursor, write_cursor, write_connection):
    counter_migrated_tuples = 0
    start = timer()

    while counter_migrated_tuples < row_cnt:
        left_tuples = row_cnt - counter_migrated_tuples
        if left_tuples > workload_size:
            limit = workload_size
        else:
            limit = left_tuples

        src_db = db_cfg.src_db['db']
        sink_db = db_cfg.sink_db['db']
        sql_select = f'SELECT id, Time, Date, SourceAddress, DestinationAddress, Data, cemi ' \
                     f'from {src_db}.knxlog ' \
                     f'LIMIT {limit} OFFSET {offset + counter_migrated_tuples}'

        read_cursor.execute(sql_select)

        prepare_migration_batch = []
        for row in read_cursor:
            snk_row = translate_one_record(row)
            prepare_migration_batch.append((str(snk_row.timestamp), str(snk_row.source_addr),
                                           str(snk_row.destination_addr), str(snk_row.apci), str(snk_row.tpci),
                                           str(snk_row.priority), snk_row.repeated, snk_row.hop_count,
                                           str(snk_row.apdu), snk_row.payload_length, str(snk_row.cemi),
                                           str(snk_row.payload_data), snk_row.is_manipulated))

        stmt = f'INSERT INTO {sink_db}.knx_dump_new (timestamp, source_addr, destination_addr, apci, tpci, priority,' \
               f'repeated, hop_count, apdu, payload_length, cemi, payload_data, is_manipulated) ' \
               f'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        write_cursor.executemany(stmt, prepare_migration_batch)
        write_connection.commit()
        counter_migrated_tuples += limit

        end = timer()
        if ((row_cnt / counter_migrated_tuples) * (end - start) - (end - start)) < 600:
            remaining_time = f'{((row_cnt / counter_migrated_tuples) * (end - start)) - (end - start):.4} seconds'
        elif ((row_cnt / counter_migrated_tuples) * (end - start) - (end - start)) > 36000:
            remaining_time = \
                f'{((row_cnt / counter_migrated_tuples) * (end - start) / 3600) - (end - start) / 3600:.4} hours'
        else:
            remaining_time = \
                f'{((row_cnt / counter_migrated_tuples) * (end - start) / 60) - (end - start) / 60:.4} minutes'
        if (end - start) < 600:
            runtime = f'{(end - start):.4} seconds'
        elif (end - start) > 36000:
            runtime = f'{((end - start) / 3600):.4} hours'
        else:
            runtime = f'{((end - start) / 60):.4} minutes'
        print(f'{(100 / row_cnt * counter_migrated_tuples):.4} % work done in {runtime} '
              f'- estimated remaining time: {remaining_time}')

    return


def translate_one_record(row):
    # Fill migrate_db-Object
    src_row = srcRow.SrcRow()

    src_row.id = row[0]
    src_row.time = row[1]
    src_row.date = row[2]
    src_row.source_address = row[3]
    src_row.destination_address = row[4]
    src_row.data = row[5]
    src_row.cemi = row[6]

    # Fill sink-Object
    sink_row = translate_to_sink_row(src_row)

    return sink_row


def translate_to_sink_row(src_row):
    sink_row = sinkRow.SinkRow()

    src_telegram = knx.parse_knx_telegram(bytes.fromhex(src_row.cemi))

    sink_row.sequence_number = 'NULL'                                   # auto-increment
    sink_row.timestamp = str(src_row.date) + " " + str(src_row.time)    # constructing datetime document from strings
    sink_row.source_addr = src_row.source_address                       # unchanged
    sink_row.destination_addr = src_row.destination_address             # unchanged
    sink_row.apci = src_telegram.apci                                   # from BaosKnxParser
    sink_row.tpci = src_telegram.tpci                                   # from BaosKnxParser
    sink_row.priority = src_telegram.priority                           # from BaosKnxParser
    sink_row.repeated = src_telegram.repeat                             # from BaosKnxParser
    sink_row.hop_count = src_telegram.hop_count                         # from BaosKnxParser
    sink_row.apdu = src_telegram.payload.hex()                          # from BaosKnxParser
    sink_row.payload_length = src_telegram.payload_length               # from BaosKnxParser
    sink_row.cemi = src_row.cemi                                        # unchanged
    sink_row.payload_data = src_telegram.payload_data                   # from BaosKnxParser
    sink_row.is_manipulated = False                                     # 0 = FALSE
    sink_row.attack_type_id = 'NULL'                                    # parsed telegrams are not considered an attack

    return sink_row


def init_db_connections():
    source_connection = None
    source_cursor = None
    sink_connection = None
    sink_cursor = None

    try:
        source_connection = mysql.connector.connect(**db_cfg.src_db)
        source_cursor = source_connection.cursor()

        # get version of database
        source_cursor.execute("SELECT VERSION()")
        db_version = source_cursor.fetchone()
        print(f'DB version: {db_version}')

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)

    try:
        sink_connection = mysql.connector.connect(**db_cfg.sink_db)
        sink_cursor = sink_connection.cursor()
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)

    return source_connection, sink_connection, source_cursor, sink_cursor


def close_db_connection(source_connection, sink_connection, source_cursor, sink_cursor):
    # Clean up
    source_cursor.close()
    source_connection.close()
    sink_cursor.close()
    sink_connection.close()

    return


src_conn, sink_conn, src_csr, snk_csr = init_db_connections()
migrate_records(0, 8000000, 10000, src_csr, snk_csr, sink_conn)
close_db_connection(src_conn, sink_conn, src_csr, snk_csr)


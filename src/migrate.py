#!/usr/bin/env python3

import pymysql
from src import srcRow
from src import sinkRow
from src import databaseconfig as db_cfg

import baos_knx_parser as knx


def migrate_records(offset, row_cnt, workload_size, read_cursor, write_cursor):
    counter_migrated_tuples = 0
    total_tuples = row_cnt - offset
    while counter_migrated_tuples < row_cnt - offset:
        left_tuples = total_tuples - counter_migrated_tuples
        if left_tuples > workload_size:
            limit = workload_size
        else:
            limit = left_tuples

        sql_select = f'SELECT id, Time, Date, SourceAddress, DestinationAddress, Data, cemi ' \
                     f'from knxlog.knxlog ' \
                     f'LIMIT {limit} OFFSET {offset + counter_migrated_tuples}'

        read_cursor.execute(sql_select)

        for row in read_cursor:
            migrate_one_record(row, write_cursor)

        counter_migrated_tuples += limit
        print(f'{(100 / total_tuples * counter_migrated_tuples):.4} % work done.')

    return


def migrate_one_record(row, sink_cursor):
    # Fill src-Object
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

    # Write sink-Row
    write_row_with_cursor(sink_row, sink_cursor)

    return


def translate_to_sink_row(src_row):
    sink_row = sinkRow.SinkRow()

    src_telegram = knx.parse_knx_telegram(bytes.fromhex(src_row.cemi))

    sink_row.sequence_number = 'NULL'                                   # auto-increment
    sink_row.timestamp = str(src_row.date) + " " + str(src_row.time)    # constructing datetime document from strings
    sink_row.source_addr = src_row.source_address                       # unchanged
    sink_row.destination_addr = src_row.destination_address             # unchanged
    sink_row.apci = src_telegram.apci                                   # from BaosKnxParser
    sink_row.priority = src_telegram.priority                           # from BaosKnxParser
    sink_row.repeated = src_telegram.repeat                             # from BaosKnxParser
    sink_row.hop_count = src_telegram.hop_count                         # from BaosKnxParser
    sink_row.apdu = src_telegram.payload                                # from BaosKnxParser
    sink_row.payload_length = src_telegram.payload_length               # from BaosKnxParser
    sink_row.cemi = src_row.cemi                                        # unchanged
    sink_row.is_manipulated = False                                     # 0 = FALSE
    sink_row.attack_type_id = 'NULL'                                    # parsed telegrams are not considered an attack

    return sink_row


def write_row_with_cursor(row, cursor):

    sql_param = f'{row.sequence_number}, ' \
                f'"{row.timestamp}", ' \
                f'"{row.source_addr}", ' \
                f'"{row.destination_addr}", ' \
                f'"{row.apci}", ' \
                f'"{row.priority}", ' \
                f'{row.repeated}, ' \
                f'{row.hop_count}, ' \
                f'"{row.apdu}", ' \
                f'{row.payload_length}, ' \
                f'"{row.cemi}", ' \
                f'{row.is_manipulated}, ' \
                f'{row.attack_type_id}'

    sql_cmd = f'INSERT INTO knx_dump VALUES ({sql_param});'
    # print(f'sql_cmd to be executed: {sql_cmd}')
    cursor.execute(sql_cmd)
    return


def init_db_connections():
    src_connection = pymysql.connect(host=db_cfg.src_db['host'],
                                     user=db_cfg.src_db['user'],
                                     passwd=db_cfg.src_db['passwd'],
                                     db=db_cfg.src_db['db'],
                                     autocommit=db_cfg.src_db['autocommit'], )
    # Connect to the database
    src_cursor = src_connection.cursor()

    sink_connection = pymysql.connect(host=db_cfg.sink_db['host'],
                                      user=db_cfg.sink_db['user'],
                                      passwd=db_cfg.sink_db['passwd'],
                                      db=db_cfg.sink_db['db'],
                                      autocommit=db_cfg.sink_db['autocommit'], )
    # Connect to the database
    sink_cursor = sink_connection.cursor()


    # request db version
    src_cursor.execute("SELECT VERSION()")
    db_version = src_cursor.fetchone()
    print(f'DB version: {db_version}')

    return (src_connection, sink_connection, src_cursor, sink_cursor)


def close_db_connection(src_connection, sink_connection, src_cursor, sink_cursor):
    # Clean up
    src_cursor.close()
    src_connection.close()
    sink_cursor.close()
    sink_connection.close()

    return


src_conn, sink_conn, src_crsr, sink_crsr = init_db_connections()
migrate_records(63790, 63810, 300, src_crsr, sink_crsr)
close_db_connection(src_conn, sink_conn, src_crsr, sink_crsr)


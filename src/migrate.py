#!/usr/bin/env python3

import pymysql
import srcRow
import sinkRow
import databaseconfig as db_cfg


def translate_to_sink_row(src_row):
    sink_row = sinkRow.SinkRow()

    sink_row.sequence_number = 0
    sink_row.timestamp = 0
    sink_row.source_addr = src_row.source_address
    sink_row.destination_addr = src_row.destination_address
    sink_row.apci = 0
    sink_row.priority = "prio"
    sink_row.flag_communication = None
    sink_row.flag_read = 0
    sink_row.flag_write = 0
    sink_row.flag_transmit = 0
    sink_row.flag_refresh = 0
    sink_row.flag_read_at_init = 0
    sink_row.repeated = 0
    sink_row.hop_count = 8
    sink_row.payload = 0
    sink_row.payload_length = 0
    sink_row.raw_package = "raw_package"
    sink_row.is_manipulated = 0
    sink_row.attack_type_id = 0

    return sink_row


def write_row_with_cursor(row, cursor):
    # todo configurations shall be external - needs improvement
    #sql = """INSERT INTO knx_dump (source_addr) VALUES('%s')""" % row.source_addr

    # print(row.sequence_number,
    #                row.timestamp,
    #                row.source_addr,
    #                row.destination_addr,
    #                row.apci,
    #                row.priority,
    #                row.flag_communication,
    #                row.flag_read,
    #                row.flag_write,
    #                row.flag_transmit,
    #                row.flag_refresh,
    #                row.flag_read_at_init,
    #                row.repeated,
    #                row.hop_count,
    #                row.payload,
    #                row.payload_length,
    #                row.raw_package,
    #                row.is_manipulated,
    #                row.attack_type_id)

    # sql = """INSERT INTO knx_dump (sequence_number,
    #                                timestamp,
    #                                source_addr,
    #                                destination_addr,
    #                                apci,
    #                                priority,
    #                                flag_communication,
    #                                flag_read,
    #                                flag_write,
    #                                flag_transmit,
    #                                flag_refresh,
    #                                flag_read_at_init,
    #                                repeated,
    #                                hop_count,
    #                                payload,
    #                                payload_length,
    #                                raw_package,
    #                                is_manipulated,
    #                                attack_type_id)
    #           VALUES('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s')""" \
    #             % (row.sequence_number,
    #                row.timestamp,
    #                row.source_addr,
    #                row.destination_addr,
    #                row.apci,
    #                row.priority,
    #                row.flag_communication,
    #                row.flag_read,
    #                row.flag_write,
    #                row.flag_transmit,
    #                row.flag_refresh,
    #                row.flag_read_at_init,
    #                row.repeated,
    #                row.hop_count,
    #                row.payload,
    #                row.payload_length,
    #                row.raw_package,
    #                row.is_manipulated,
    #                row.attack_type_id)
#    sql = """INSERT INTO knx_dump (source_addr, hop_count) VALUES('%s,%s')""" % (row.source_addr, row.hop_count)
    sql = """INSERT INTO knx_dump (source_addr) VALUES('%s')""" % (row.source_addr)


    cursor.execute(sql)
    return

def migrate_one_record(row):
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
    sink_row = sinkRow.SinkRow()
    sink_row = translate_to_sink_row(src_row)

    # Write sink-Row
    write_row_with_cursor(sink_row, sink_cursor)

    return


def migrate_records(offset, row_cnt, read_cursor, write_cursor):
    # todo configurations shall be external - needs improvement
    read_cursor.execute("""SELECT * FROM knxlog LIMIT %s,%s""" % (offset, row_cnt))

    for row in read_cursor:
        migrate_one_record(row)

    return

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
print("DB version: %s " % db_version)

# Example read from DB
#sql = """SELECT * FROM knxlog.knxlog LIMIT 1000"""
#src_cursor.execute(sql)
#for response in src_cursor:
#    print(response)

migrate_records(0, 10, src_cursor, sink_cursor)

# Clean up
src_cursor.close()
src_connection.close()
sink_cursor.close()
sink_connection.close()



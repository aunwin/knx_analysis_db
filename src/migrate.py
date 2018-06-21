#!/usr/bin/env python3

import pymysql
import srcRow
import sinkRow
import databaseconfig as db_cfg

import baos_knx_parser as knx

def translate_to_sink_row(src_row):
    sink_row = sinkRow.SinkRow()

    #print(src_row.cemi)
    src_telegram = knx.parse_knx_telegram(bytes.fromhex(src_row.cemi))
    print(type(src_telegram)) #todo why is it a KnxStandardTelegram when I expect it to be an Extended Telegram
    print(src_telegram)

    print('telegram_type: %s' % src_telegram.telegram_type)
    print('ack: %s' % src_telegram.ack)
    print('confirm: %s' % src_telegram.confirm)
    print('apci: %s' % src_telegram.apci)
    print('tpci: %s' % src_telegram.tpci)
    print('packet_number: %s' % src_telegram.packet_number)
    #print('Debug print from translate_to_sink_row( %s )' % src_row)


    # todo delete hardcoded dummy sink_row
    sink_row.sequence_number = "NULL"                                   # auto-increment
    sink_row.timestamp = str(src_row.date) + " " + str(src_row.time)    # constructing datetime document from strings
    sink_row.source_addr = src_row.source_address                       # unchainged
    sink_row.destination_addr = src_row.destination_address             # unchainged
    sink_row.apci = int(src_telegram.apci)                              # todo db requires int but string would be fare more readable
    #print(sink_row.apci)                                               # todo since 'A_GROUP_VALUE_WRITE' is more helpful than '128'
    sink_row.priority = src_telegram.priority                           # TelegramPriority from BaosKnxParser
    # todo #Kommunikationsobjektflags
    sink_row.flag_communication = 0
    sink_row.flag_read = 0
    sink_row.flag_write = 0
    sink_row.flag_transmit = 0
    sink_row.flag_refresh = 0
    sink_row.flag_read_at_init = 0
    sink_row.repeated = src_telegram.repeat                             # from Parser
    sink_row.hop_count = src_telegram.hop_count                         # from Parser
    sink_row.payload = src_row.cemi                                     #todo is payload supposed to be cemi? No! cemi = raw package | sink_row.payload should be renamed to sink_row.apdu
    sink_row.payload_length = src_telegram.payload_length               # from Parser
    sink_row.raw_package = src_row.cemi                                 #todo rename raw_packege into cemi
    sink_row.is_manipulated = False                                     # 0 = FALSE
    sink_row.attack_type_id = 0                                         # todo to be determined

    return sink_row


def write_row_with_cursor(row, cursor):
    # todo configurations shall be external - needs improvement
    # print(row.sequence_number,
    #                row.timestamp,
    #                row.source_addr,
    #                row.destination_addr,
    #                row.apci,q
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


    # sql_static_example = """INSERT INTO knx_dump VALUES (NULL,               #sequence_number
    #                                       CURRENT_TIMESTAMP,  #timestamp
    #                                       "0.0.0",            #source_addr
    #                                       "1/1/1",            #destination_addr
    #                                       1,                  #apci
    #                                       "prio",             #priority
    #                                       1,                  #flag_communication
    #                                       1,                  #flag_read
    #                                       1,                  #flag_write
    #                                       1,                  #flag_transmit
    #                                       1,                  #flag_refresh
    #                                       1,                  #flag_read_at_init
    #                                       1,                  #repeated
    #                                       8,                  #hop_count
    #                                       "payload",          #payload
    #                                       1,                  #payload_length
    #                                       "raw",              #raw_package
    #                                       1,                  #is_manipulated
    #                                       NULL                #attack_type_id
    #                                       );"""


    sql_param = '{row.sequence_number}, ' \
                '"{row.timestamp}", ' \
                '"{row.source_addr}", ' \
                '"{row.destination_addr}",' \
                '{row.apci}, ' \
                '"{row.priority}",' \
                '{row.flag_communication},' \
                '{row.flag_read},' \
                '{row.flag_write},' \
                '{row.flag_transmit},' \
                '{row.flag_refresh},' \
                '{row.flag_read_at_init},' \
                '{row.repeated},' \
                '{row.hop_count},' \
                '"{row.payload}",' \
                '{row.payload_length},' \
                '"{row.raw_package}",' \
                '{row.is_manipulated},' \
                '{row.attack_type_id}'\
                .format(row=row)

    sql_cmd = "INSERT INTO knx_dump VALUES (%s);" % sql_param
    print('sql_cmd to be executed: \n%s' % sql_cmd)

    cursor.execute(sql_cmd)
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
    # todo configurations should better be external

    sql_select = """SELECT id, Time, Date, SourceAddress, DestinationAddress, Data, cemi 
                        from knxlog.knxlog 
                        LIMIT %s,%s""" % (offset, row_cnt)
    read_cursor.execute(sql_select)

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


migrate_records(0, 1, src_cursor, sink_cursor)

# Clean up
src_cursor.close()
src_connection.close()
sink_cursor.close()
sink_connection.close()



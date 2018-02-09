#!/usr/bin/env python3

import pymysql
import srcLog
import destDump
import databaseconfig as db_cfg

src_connection = pymysql.connect(host=db_cfg.mysql['host'],
                                 user=db_cfg.mysql['user'],
                                 passwd=db_cfg.mysql['passwd'],
                                 db=db_cfg.mysql['db'],
                                 autocommit=db_cfg.mysql['autocommit'],)
# Connect to the database
src_cursor = src_connection.cursor()

# request db version
src_cursor.execute("SELECT VERSION()")
db_version = src_cursor.fetchone()
print("DB version: %s " % db_version)

# Clean up
src_cursor.close()
src_connection.close()
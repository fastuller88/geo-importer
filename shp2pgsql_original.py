# -*- coding: utf-8 -*-
import subprocess
import util
from importer_modes import *
import config
import psycopg2
import os
import sys
reload(sys)  
sys.setdefaultencoding('latin1')
conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % \
        (config.db['host'], config.db['name'], config.db['user'], config.db['password']))

def shape_to_pgsql(conn, shape_path, table, mode, srid=-1, log_file=None, batch_size=1000):
    result = False
    modeflags = {
        str(IMPORT_MODE_CREATE): "c",
        str(IMPORT_MODE_APPEND): "a",
        str(IMPORT_MODE_STRUCTURE): "p",
        str(IMPORT_MODE_DATA): "",
        str(IMPORT_MODE_SPATIAL_INDEX): ""
    }

    args = [
        config.shp2pgsql,
        "-%s" % ''.join([modeflags[f] for f in modeflags.keys() if int(f) & mode]),
        # para solo importar la data
        #"-d",
        "-W", "latin1",
        "-g", "the_geom",
        "-s", str(srid),
        shape_path,
        table]
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=log_file)

    cursor = conn.cursor()
    try:
        with p.stdout as stdout:
            for commands in util.groupsgen(util.read_until(stdout, ';'), batch_size):
                command = ''.join(commands).strip()
                if len(command) > 0:
                    cursor.execute(command)
        conn.commit()     
        result = True
        #util.write_outputs('geoimporter: Successful import: ' + shape_path)
        print 'geoimporter: Successful import', table
    except:
        error = 'geoimporter: Error import', table, 'for folder', shape_path, 'SRID', srid
        util.write_outputs( 'geoimporter: Error import ' + table + ' for folder' + shape_path + ' SRID' + srid)
        print error
        conn.rollback()
        result = False
        #raise
    finally:
        cursor.close()
        return result

def vacuum_analyze(conn, table):
    isolation_level = conn.isolation_level
    conn.set_isolation_level(0)
    cursor = conn.cursor()
    try:
        cursor.execute('vacuum analyze %s;' % table)
    finally:
        cursor.close()
        conn.set_isolation_level(isolation_level)

def run(shape_file, table, crs):
    shape_file = shape_file.replace("\\", "/")
    if table == '':
        table = os.path.splitext(os.path.split(shape_file)[1])[0]
    result = shape_to_pgsql(conn, shape_file, table, IMPORT_MODE_CREATE + IMPORT_MODE_DATA + IMPORT_MODE_SPATIAL_INDEX, crs)
    if result: vacuum_analyze(conn, table)    
    return result


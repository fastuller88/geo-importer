# -*- coding: utf-8 -*-
db = {
    'host': '10.10.201.4',
    'name': 'gis_db',
    'user': 'gis_admin',
    'password': 'test'
}

# None means no log, otherwise name of log (mostly shp2pgsql's output)
log_file = None

# Path to shp2pgsql
shp2pgsql = 'C:/Program Files/PostgreSQL/9.4/bin/shp2pgsql.exe'

gsd = {
    # Path to GSD's Shapefiles root
    'path': 'D:/uploads',
    # The SRID of the delivered data
    'srid': 32717
}

# Conection mongodb
uri = 'mongodb://admin:12345678@10.10.201.4:27017/?authSource=pdot&authMechanism=SCRAM-SHA-1'

# Data catalog
path_esquemas = 'D:/ProjectsWeb/import-shapes/geoimporter/data.json'

# Path where are geo files
#path_geo_files = 'D:/examples_geo'
path_geo_files = 'D:/uploads'


# Path where are commands pgsql
path_command_pgsql = 'C:/Program Files/PostgreSQL/9.4/bin/' 



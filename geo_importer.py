#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pyunpack import Archive
import pprint
import os
import patoolib
import shutil
import uuid
import psycopg2
import shp2pgsql
import glob
import json
import datetime
import config
import util
import sys
from osgeo import ogr
import unidecode
import re
reload(sys)  
sys.setdefaultencoding('latin1')

TEMP_FOLDER = "tmp"
PATH_TEMP = os.path.join(config.path_geo_files, TEMP_FOLDER)
ZIP_LIST = os.listdir(config.path_geo_files)
total_imported_geo = 0
total_no_imported_geo = 0
no_in_file = 0
issue_extraction = 0
driver = ogr.GetDriverByName('ESRI Shapefile')

def mongodb_test_conn(client):
	try:
	   # The ismaster command is cheap and does not require auth.
		client.admin.command('ismaster')
	except ConnectionFailure:
		print("Server not available")

def mongdb_conn():
	client = MongoClient(config.uri)
	#client = MongoClient(host='localhost', port=27017)
	mongodb_test_conn(client)
	return client

def is_file_in_folder(name = ''):
	return name in (ZIP_LIST)

def create_schema_postgres(schema_name):
	sql = 'CREATE SCHEMA IF NOT EXISTS {}'.format(schema_name)
	cur.execute(sql)
	conn.commit()

def extract_file(name_file):
	full_path = os.path.join(config.path_geo_files, name_file)
	name_folder_shp = str(uuid.uuid4())
	path_folder_shp = os.path.join(PATH_TEMP, name_folder_shp)
	os.makedirs(path_folder_shp)
	try:
		patoolib.extract_archive(full_path, outdir=path_folder_shp)
	except:
		global issue_extraction
		issue_extraction+=1
		error =  'geoimporter: Error in extration file: ' + str(name_file)
		print error
		util.write_outputs(error)
	return path_folder_shp

def shape_importer(folder, file, esquemas):
	#loop through each of the subdirectories, open and process each shapefile
	folder_root = os.path.join(folder, "*")
	for dir in glob.glob(folder_root):
		#print ('Looking folder o file ', dir)
		isFolder = os.path.isdir(dir)
		if isFolder:
			#print ('Looking for subfolder')
			shape_importer(dir, file, esquemas)
		else:
				split_dir = dir.split('\\')
				full_name_file = split_dir[(len(split_dir)) - 1]
				split_full_name = full_name_file.split('.')
				name_file = split_full_name[0]
				try:
					extension = split_full_name[1]
				except:
					extension = '.nada'
				#print 'aaaaaaaaa', split_full_name
				if extension == 'shp' and len(split_full_name) == 2:
					print 'geoimporter: Info shape encontrado', name_file
					clean_name = clean_file_name(name_file)
					table_name = build_name_table(file, esquemas, clean_name)	
					full_path_shape = os.path.join(folder, full_name_file)
					#print 'dddddddddd', extension, full_path_shape	
					crs = get_crs(full_path_shape)	
					is_import = shp2pgsql.run(full_path_shape, table_name, crs)
					global total_imported_geo
					global total_no_imported_geo
					if is_import: total_imported_geo+=1
					else: total_no_imported_geo+=1
					#break

def publish_geo(files, esquemas):
	for file in files.find({'ARCHIVO':{'$regex': 'geo'}}):
		name_file = file['ARCHIVO']
		result = is_file_in_folder(name_file)
		if not result:
			error = 'geoimporter: Warning: Dont found file'+name_file+'Dont matching table mongodb and file in folder'
			print error 
			util.write_outputs(error)
			global no_in_file
			no_in_file +=1
		else:

			#try:
			path_folder_shp = extract_file(name_file)
			shape_importer(path_folder_shp, file, esquemas)
			#except:
				#error = 'geoimporter: Error: Something was wrong with' + name_file
				#print error 
				#util.write_outputs(error)
				#continue
			
def get_crs(path):
	try:
		shape = driver.Open(path)
		layer= shape.GetLayer()
		crs = layer.GetSpatialRef()
		# from Geometry
		#feature = layer.GetNextFeature()
		#geom = feature.GetGeometryRef()
		#spatialRef = geom.GetSpatialReference()    

		srid = crs.GetAttrValue("AUTHORITY", 1)
	#except AttributeError:
	except:
		srid = 32717
	return srid

def dpa2level(length):
	level = {2: 'provincia', 4: 'canton', 6:'parroquia'}
	return level[length]

def build_name_table(file, esquemas, name):
	name_file = file['ARCHIVO']
	dpa = file['GAD']
	descripcion = file['DESCRIPCION']
	try: 
		escala = file['ESCALA']
	except: 
		escala = '0K'
	tipo = dpa2level(len(dpa))
	for row in esquemas:
		if row['tipo'] == tipo and row['descripcion'].strip() == descripcion.strip():
			prefijo = row['prefijo']
			now = datetime.datetime.now()
			mes = now.month
			if int(mes) < 10 : mes = '0'+str(mes) 
			else: mes = str(mes)
			table_name = prefijo.format(dpa, now.year, mes, escala) + '_'+name
			table_name = row['esquema']+'.'+table_name[:60]
			create_schema_postgres(row['esquema'])
			return table_name

def clean_file_name(name):
	u = unicode(name, "latin-1")
	a = unidecode.unidecode(u).replace(' ','').replace('_','').lower().replace('-','')
	return re.sub('[^ a-zA-Z0-9]', '', a)


def print_results():
	texto = ('*'*50 +'\nNumber of files what does not find in folder: '+str(no_in_file) 
	+'\nNumber of files stored in BD: ' + str(total_imported_geo)
	+'\nNumber of files does not sotres in BD: ' + str(total_no_imported_geo)
	+'\nNumber issues extraction: ' + str(issue_extraction) + '\n'
	+'*'*50)
	print texto	
	util.write_outputs(texto)	


def main():
	json_esquemas = open(config.path_esquemas)
	esquemas_str = json_esquemas.read()
	esquemas = json.loads(esquemas_str)	
	client = mongdb_conn()
	db = client.pdot
	files = db.files
	util.create_folder_temp(PATH_TEMP)
	publish_geo(files, esquemas)
	print_results()
	print 'Completed!!!'


if __name__ == '__main__':
	try:
		conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % \
		(config.db['host'], config.db['name'], config.db['user'], config.db['password']))
		print ("Connect to database postgis")
	except:
		print ("Can't connect to database postgis")
	cur = conn.cursor()		
	main()
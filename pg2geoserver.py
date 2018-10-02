from geoserver.catalog import Catalog

cat = Catalog("http://10.10.201.33/geoserver/rest/", "admin", "geoserver")

schemas = ["a_industria_servicios", 'b_geografia_socioeconomica', 'c_infraestructura_transporte', 'h_demarcacion']
workspace = "geonode"
native_crs = 'EPSG:32717'
for schema in schemas:
	print '*'*50
	ds = cat.create_datastore(schema, workspace)
	ds.connection_parameters.update(
	    host="10.10.201.4",
	    port="5432",
	    database="gis_db",
	    user="gis_admin",
	    passwd="test",
	    dbtype="postgis",
	    schema=schema)
	try:
		cat.save(ds)
	except:
		print schema + ' datastore already exist'
		#ds = cat.get_store(schema)
	finally:
		all_layers = ds.get_resources(available=True)
		i = 0
		for layer in all_layers:
			#print layer
			try:
				ft = cat.publish_featuretype(layer, ds, native_crs)
				i+=1
			except:
				print 'Dont import:', layer
				next
		print 'Completed', i, 'of', len(all_layers), 'from', schema
		#cat.save(ft)

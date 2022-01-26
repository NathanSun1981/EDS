#!/usr/bin/python3
from ast import parse
from flask import Flask, request, jsonify, redirect, send_from_directory, send_file
from flask_restful import Resource, Api
from json import dumps, loads
import io
import os
import sys
import time
import shutil
import psycopg2
import snappy


#sys.path.append('.')
#sys.path.append('..')

rootdir = r'\home'

conn = psycopg2.connect(
    host="localhost",
    database="sentinel2",
    port="5432",
    user="postgres",
    password="postgres")

cur = conn.cursor()

condition_columns = ["CLOUD_COVERAGE_ASSESSMENT", "DARK_FEATURES_PERCENTAGE", "CLOUD_SHADOW_PERCENTAGE", \
                    "VEGETATION_PERCENTAGE", "NOT_VEGETATED_PERCENTAGE", \
                    "WATER_PERCENTAGE", "UNCLASSIFIED_PERCENTAGE", "SNOW_ICE_PERCENTAGE"]

#db_connect = create_engine('sqlite:///Database/tasks.db', connect_args={'check_same_thread':False}, poolclass=StaticPool)
#onn = db_connect.connect() 

app = Flask(__name__)
api = Api(app)

        
class Query(Resource):  

    def querySen2(self,args):
        #PRODUCT_NAME, PRODUCT_URI, PROCESSING_LEVEL, SPACECRAFT_NAME,  GENERATION_TIME, CLOUD_COVERAGE_ASSESSMENT, \
        #DARK_FEATURES_PERCENTAGE, CLOUD_SHADOW_PERCENTAGE, VEGETATION_PERCENTAGE, NOT_VEGETATED_PERCENTAGE, WATER_PERCENTAGE, UNCLASSIFIED_PERCENTAGE, \
        #SNOW_ICE_PERCENTAGE, IMAGE_FILE, FOOTPRINT
        query = "SELECT PRODUCT_NAME, IMAGE_FILE FROM SEN2INFO WHERE "
        if "PROCESSING_LEVEL" in args:  
            query = query + "PROCESSING_LEVEL = '%s'" % args['BATCHACCOUNTURL'] + " AND "

        if "SPACECRAFT_NAME" in args:  
            query = query + "SPACECRAFT_NAME = '%s'" % args['SPACECRAFT_NAME'] + " AND "

        if "DATE" in args:
            if isinstance(args['DATE'], str):      
                obj = loads(args['DATE'])
            else:
                obj = args['DATE']
            if "lt" in obj:
                query = query + "GENERATION_TIME < '%s'" % obj['lt'] + " AND "
            if "gt" in args['DATE']:
                query = query + "GENERATION_TIME > '%s'" % obj['gt'] + " AND "


        for condition_column in condition_columns:
            if condition_column in args:
                if isinstance(args[condition_column], str):      
                    obj = loads(args[condition_column])
                else:
                    obj = args[condition_column]
                print(obj)
                if "lt" in args[condition_column]:
                    query = query + "%s < %f" % (condition_column, float(obj['lt'])) + " AND "
                if "gt" in args[condition_column]:
                    query = query + "%s > %f" % (condition_column, float(obj['gt'])) + " AND "


        #find zip file according to the conditions, and then find sub-files according to 
        query = query[:-5]+ ";"
        #print(query)

        try:
            cur.execute(query)
            rows = cur.fetchall()
            #print (rows)
        except:
            print("query error")
            return "query error"

        file_dic = {}

        for row in rows:
            #find the path and then find the 
            print (row[0])
            #find path
            query = "SELECT PRODUCT_URI FROM SEN2PATH WHERE PRODUCT_NAME = '%s'" % row[0]
            try:
                cur.execute(query)
                url = cur.fetchone()
                #print (url[0])
            except:
                print("query error")
                return "query error"
            
            #here for sen2 and sen3 url will end up with SAFE.
            file_dic[row[0]] = '/'.join(url[0].split('/')[:-1]) + '/' + row[0] + '.zip'


            files = []
            if "IMAGE_FILE" in args:  
                file_keywords = args['IMAGE_FILE'].split(',')
                for keyword in file_keywords:
                    #print(keyword)                   
                    for element in row[1]:
                        #print(element)
                        if keyword in element:
                            files.append(url[0]+ '/' + element + '.jp2')
                #print(files)
                file_dic[row[0]] = files
        #print (file_dic)

        if "POLYGON" in args:
            print(args['POLYGON'])
            query = "SELECT PRODUCT_NAME FROM geometries WHERE ST_Intersects(GEOM, 'SRID=4326; POLYGON(%s)');" % args['POLYGON']
            print(query)
            try:
                cur.execute(query)
                urls = cur.fetchall()
            except:
                print("query error")
                return "query error"
            file_names = []
            for url in urls:
                print(url[0])
                file_names.append(url[0])

        keys = file_dic.keys() & file_names
        result = {k:file_dic[k] for k in keys} 


        return jsonify(result)

   
    def get(self):
        print(request.args)
        return self.querySen2(request.args)


    def post(self):
        print(request.json)    
        return self.querySen2(request.json) 

class Download(Resource):  
    
    def get(self):
        #print(request.args['path'])
        filename= request.args['path'].split(os.sep)[-1]
        dirpath= request.args['path'][:-len(filename)]
        print(dirpath, filename)
        return send_from_directory(dirpath, filename, as_attachment=True)
    
        
    
api.add_resource(Download, '/download')
api.add_resource(Query, '/query') 




if __name__ == '__main__':
    app.run(host='0.0.0.0', threaded=True)
    #app.run()




    

#!/usr/bin/python3
import io
import os
import sys
import time
import shutil
from typing import Container
import psycopg2
import xml.etree.ElementTree as ET


sys.path.append('.')
sys.path.append('..')

conn = psycopg2.connect(
    host="localhost",
    database="sentinel2",
    port="5432",
    user="postgres",
    password="postgres")
cur = conn.cursor()

def findxmls(xmlName):
    file_arr = []
    for root, dirs, files in os.walk("/home/ubuntu/project/data/MSI/"):
        #path = root.split(os.sep)
        #print((len(path) - 1) * '---', os.path.basename(root))
        for file in files:
            if file == xmlName:
                file_arr.append(root +'/'+ file)
    print (file_arr)
    return file_arr

def xml2table(xmlfilem):
    print ('start to import xml to database')
    #read xml
    tree = ET.parse(xmlfilem)
    root = tree.getroot()
    #print(root.tag, root.attrib)

    content = {}

    #INSERT INTO SEN2PATH(column1, column2, …) VALUES (value1, value2, …);

    for GENERATION_TIME in root.iter('GENERATION_TIME'):
        #print(GENERATION_TIME.text)
        content['GENERATION_TIME'] = GENERATION_TIME.text[0:10]

    for PRODUCT_URI in root.iter('PRODUCT_URI'):
        #print(PRODUCT_URI.text)
        content['PRODUCT_URI'] = PRODUCT_URI.text
        content['PRODUCT_NAME'] = PRODUCT_URI.text.split('.')[0]

    for PROCESSING_LEVEL in root.iter('PROCESSING_LEVEL'):
        #print(PROCESSING_LEVEL.text)
        content['PROCESSING_LEVEL'] = PROCESSING_LEVEL.text

    for SPACECRAFT_NAME in root.iter('SPACECRAFT_NAME'):
        #print(SPACECRAFT_NAME.text)
        content['SPACECRAFT_NAME'] = SPACECRAFT_NAME.text
    
    for Cloud_Coverage_Assessment in root.iter('Cloud_Coverage_Assessment'):
        #print(Cloud_Coverage_Assessment.text)
        content['CLOUD_COVERAGE_ASSESSMENT'] = float(Cloud_Coverage_Assessment.text)
    
    for DARK_FEATURES_PERCENTAGE in root.iter('DARK_FEATURES_PERCENTAGE'):
        #print(DARK_FEATURES_PERCENTAGE.text)
        content['DARK_FEATURES_PERCENTAGE'] = float(DARK_FEATURES_PERCENTAGE.text)

    for CLOUD_SHADOW_PERCENTAGE in root.iter('CLOUD_SHADOW_PERCENTAGE'):
        #print(CLOUD_SHADOW_PERCENTAGE.text)
        content['CLOUD_SHADOW_PERCENTAGE'] = float(CLOUD_SHADOW_PERCENTAGE.text)

    for VEGETATION_PERCENTAGE in root.iter('VEGETATION_PERCENTAGE'):
        #print(VEGETATION_PERCENTAGE.text)
        content['VEGETATION_PERCENTAGE'] = float(VEGETATION_PERCENTAGE.text)

    for NOT_VEGETATED_PERCENTAGE in root.iter('NOT_VEGETATED_PERCENTAGE'):
        #print(NOT_VEGETATED_PERCENTAGE.text)
        content['NOT_VEGETATED_PERCENTAGE'] = float(NOT_VEGETATED_PERCENTAGE.text)

    for WATER_PERCENTAGE in root.iter('WATER_PERCENTAGE'):
        #print(WATER_PERCENTAGE.text)
        content['WATER_PERCENTAGE'] = float(WATER_PERCENTAGE.text)

    for UNCLASSIFIED_PERCENTAGE in root.iter('UNCLASSIFIED_PERCENTAGE'):
        #print(UNCLASSIFIED_PERCENTAGE.text)
        content['UNCLASSIFIED_PERCENTAGE'] = float(UNCLASSIFIED_PERCENTAGE.text)

    for SNOW_ICE_PERCENTAGE in root.iter('SNOW_ICE_PERCENTAGE'):
        #print(SNOW_ICE_PERCENTAGE.text)
        content['SNOW_ICE_PERCENTAGE'] = float(SNOW_ICE_PERCENTAGE.text)
    
    #image_files = ""
    image_files = []
    for IMAGE_FILE in root.iter('IMAGE_FILE'):
        image_files.append(IMAGE_FILE.text)
        #image_files = image_files + IMAGE_FILE.text + ";"
    content['IMAGE_FILE'] = image_files

    for EXT_POS_LIST in root.iter('EXT_POS_LIST'):
        #print(EXT_POS_LIST.text)
        footprint = []
        print(EXT_POS_LIST.text)
        points = EXT_POS_LIST.text.split()
        for i in range(0, len(points), 2):
            footprint.append(points[i+1] + ' ' + points[i])
        content['EXT_POS_LIST'] = ','.join(footprint)
        #print(content['EXT_POS_LIST'])

    #print(content)
    return content

def lst2pgarr(alist):
    return '{' + ','.join(alist) + '}'


def savepath(xmlfilem, content):
    #save file path
    query = "INSERT INTO SEN2PATH (PRODUCT_NAME, PRODUCT_URI) VALUES ('%s', '%s') ON CONFLICT (PRODUCT_NAME) DO NOTHING;" % (content['PRODUCT_NAME'], '/'.join(xmlfile.split('/')[:-1]))
    #print(query)
    try:               
        cur.execute(query)
        conn.commit()
    except:
        print("cannot insert sen2path table!")


def saveinfo(content):
    #save file info
    query = "INSERT INTO SEN2INFO (PRODUCT_NAME, PRODUCT_URI, PROCESSING_LEVEL, SPACECRAFT_NAME,  GENERATION_TIME, CLOUD_COVERAGE_ASSESSMENT, \
        DARK_FEATURES_PERCENTAGE, CLOUD_SHADOW_PERCENTAGE, VEGETATION_PERCENTAGE, NOT_VEGETATED_PERCENTAGE, WATER_PERCENTAGE, UNCLASSIFIED_PERCENTAGE, \
        SNOW_ICE_PERCENTAGE, IMAGE_FILE, FOOTPRINT) VALUES \
        ('%s', '%s', '%s', '%s', '%s', %f, %f, %f, %f, %f, %f, %f, %f, '%s','%s') \
        ON CONFLICT (PRODUCT_NAME) DO NOTHING;" % (content['PRODUCT_NAME'], content['PRODUCT_URI'], content['PROCESSING_LEVEL'], content['SPACECRAFT_NAME'], content['GENERATION_TIME'],
        content['CLOUD_COVERAGE_ASSESSMENT'], content['DARK_FEATURES_PERCENTAGE'], content['CLOUD_SHADOW_PERCENTAGE'], content['VEGETATION_PERCENTAGE'], content['NOT_VEGETATED_PERCENTAGE'], content['WATER_PERCENTAGE'], content['UNCLASSIFIED_PERCENTAGE'], content['SNOW_ICE_PERCENTAGE'], lst2pgarr(content['IMAGE_FILE']), content['EXT_POS_LIST'])
    #print(query)
    try:
        cur.execute(query, content)
        conn.commit()
    except:
        print("cannot insert sen2info table!")

def savepolygon(filename, polygon):
    #save file geoinfo to post gis

    query = "INSERT INTO geometries VALUES('%s', ST_GeomFromText('POLYGON((%s))', 4326)) ON CONFLICT (PRODUCT_NAME) DO NOTHING;" % (content['PRODUCT_NAME'], content["EXT_POS_LIST"])
    print(query)
    try:
        cur.execute(query, content)
        conn.commit()
    except:
        print("cannot insert sen2info table!")



if __name__ == '__main__':

    try:
        cur.execute("CREATE TABLE IF NOT EXISTS SEN2PATH(\
            PRODUCT_NAME VARCHAR(80) NOT NULL,\
            PRODUCT_URI TEXT NOT NULL,\
            UNIQUE(PRODUCT_NAME)\
            );" )        
        conn.commit()    
        filearr = findxmls('MTD_MSIL2A.xml')
    except:
        print("cannot create SEN2PATH table!")

    try:
        cur.execute("CREATE TABLE IF NOT EXISTS SEN2INFO(\
            PRODUCT_NAME VARCHAR(80) NOT NULL,\
            PRODUCT_URI TEXT NOT NULL,\
            PROCESSING_LEVEL VARCHAR(20) NOT NULL,\
            SPACECRAFT_NAME VARCHAR(20) NOT NULL,\
            GENERATION_TIME DATE NOT NULL,\
            CLOUD_COVERAGE_ASSESSMENT DECIMAL,\
            DARK_FEATURES_PERCENTAGE DECIMAL,\
            CLOUD_SHADOW_PERCENTAGE DECIMAL,\
            VEGETATION_PERCENTAGE DECIMAL,\
            NOT_VEGETATED_PERCENTAGE DECIMAL,\
            WATER_PERCENTAGE DECIMAL,\
            UNCLASSIFIED_PERCENTAGE DECIMAL,\
            SNOW_ICE_PERCENTAGE DECIMAL,\
            IMAGE_FILE TEXT[],\
            FOOTPRINT TEXT,\
            UNIQUE(PRODUCT_NAME)\
            );" )
        conn.commit()  
    except:
        print("cannot create SEN2INFO table!")

    try:
        cur.execute("CREATE TABLE IF NOT EXISTS geometries (PRODUCT_NAME VARCHAR(80) NOT NULL, GEOM GEOMETRY, UNIQUE(PRODUCT_NAME));")
        conn.commit()  
    except:
        print("cannot create SEN2INFO table!")
        
    for xmlfile in filearr:
        content = xml2table(xmlfile) 
        #savepath(xmlfile, content)  
        #saveinfo(content)
        savepolygon(content['PRODUCT_NAME'], content['EXT_POS_LIST'])
    

    
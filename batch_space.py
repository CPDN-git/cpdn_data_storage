#!/usr/bin/env python2.7
#-----------------------------------------------------------------------
# Program: project_batch_stats.py 
# Purpose: To display queue by project for open batches.
# Created: Sarah Sparrow 22/03/16
#-----------------------------------------------------------------------
import os, sys, time, MySQLdb, hashlib
import numpy as np
import collections
from xml.sax.saxutils import escape  # needed to make our XML safe.
import xml.etree.ElementTree as ET
import matplotlib
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import csv
from socket import gethostname

# List of outcome, client_state, server_state for reference
outcome={'0':'RESULT_OUTCOME_INIT',\
'1' : 'RESULT_OUTCOME_SUCCESS',\
'2' : 'RESULT_OUTCOME_COULDNT_SEND',\
'3' : 'RESULT_OUTCOME_CLIENT_ERROR',\
'4' : 'RESULT_OUTCOME_NO_REPLY',\
'5' : 'RESULT_OUTCOME_DIDNT_NEED',\
'6' : 'RESULT_OUTCOME_VALIDATE_ERROR',\
'7' : 'RESULT_OUTCOME_CLIENT_DETACHED' }

client_state={'0' : 'RESULT_NEW',\
'1' : 'RESULT_FILES_DOWNLOADING',\
'2' : 'RESULT_FILES_DOWNLOADED',\
'3' : 'RESULT_COMPUTE_ERROR',\
'4' : 'RESULT_FILES_UPLOADING',\
'5' : 'RESULT_FILES_UPLOADED',\
'6' : 'RESULT_ABORTED'}

server_state={'1' : 'RESULT_SERVER_STATE_INACTIVE',\
'2' : 'RESULT_SERVER_STATE_UNSENT',\
'3' : 'Unsent_seq',\
'4' : 'RESULT_SERVER_STATE_IN_PROGRESS',\
'5' : 'RESULT_SERVER_STATE_OVER'}

today=time.strftime('%Y-%m-%d')

class BatchDB:
	#database connection info - starts empty
	pass

def ParseConfig(xmlfilename):
	doc        = ET.parse(xmlfilename)
	root_node = doc.getroot()

	BatchDB.host=root_node.findtext('db_host')
	BatchDB.batchUser=root_node.findtext('batch_user')
	BatchDB.batchPass=root_node.findtext('batch_passwd')
	BatchDB.name=root_node.findtext('db_name')
	BatchDB.dbexpt=root_node.findtext('db_name')
	BatchDB.dbboinc=root_node.findtext('boinc_db_name')

def query_database(ulserver):
	db = MySQLdb.connect(BatchDB.host,BatchDB.batchUser,BatchDB.batchPass,BatchDB.name )
	cursor = db.cursor(MySQLdb.cursors.DictCursor)

	query_batches ='select p.name as Project, b.id as Batch, b.name as "Batch Name", SUBSTRING_INDEX(b.owner,"<",1) as Owner, b.archive_status as Status, b.ended as Ended, p.data_expiry_date as "Data Expiry",b.current_location  as Location from '+BatchDB.dbexpt+'.cpdn_batch b left join '+BatchDB.dbexpt+'.cpdn_project p on b.projectid=p.id order by p.name, SUBSTRING_INDEX(b.owner,"<",1),b.id, b.archive_status;'
        

	# Get numbers of results in different states.
	cursor.execute(query_batches)
	batch_list=cursor.fetchall()
	cursor.close()
	return batch_list

def read_size_csv(ulserver):
        batch_size_dict={}
        with open(ulserver+'_storage.csv', 'rb') as csvfile:
            batch_size_reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
            header=True
            for row in batch_size_reader:
                if header:
                    print "Reading header data"
                    header=False
                else:
                    data=row[0].split(",")
                    batchno=str(data[0])
                    batch_size_dict[batchno]=float(data[1])
        return batch_size_dict

def batch_space(ulserver):
	batch_list=query_database(ulserver)
        batch_size_dict=read_size_csv(ulserver)

        with open(ulserver+"_data_usage.csv", 'wb') as csvfile:
           batch_sizes = csv.writer(csvfile,delimiter=',') 
           batch_sizes.writerow(["Project","Owner","Batch","Batch Name","Size (Tb)","Status","Closed","Data Expiry Date"])
           for i in range(0,len(batch_list)):
                project=str(batch_list[i]['Project'])
                batchno=str(batch_list[i]['Batch'])
                batch_name=str(batch_list[i]['Batch Name'])
                owner=str(batch_list[i]['Owner'])
                status=str(batch_list[i]['Status'])
                closed=bool(batch_list[i]['Ended'])
                data_expiry=str(batch_list[i]['Data Expiry'])
           
                try:
                    batch_sizes.writerow([project,owner,batchno,batch_name,batch_size_dict[batchno],status,closed,data_expiry])
                except:
                    print "Batch "+batchno+" not found on "+ulserver
	

def main():
	print time.strftime("%Y/%m/%d %H:%M:%S") + " Starting project_batch_stats.py"
	project=os.environ.get('CPDN_PROJECT')
	host=gethostname()
	if project=='CPDN' or host=='sepia':
		config='/storage/www/cpdnboinc/ancil_batch_user_config.xml'
		out_path='/storage/www/cpdnboinc/html/user/'
	elif project=='CPDN_DEV':
		config='/storage/boinc/projects/cpdnboinc_dev/ancil_batch_user_config.xml'
		out_path='/storage/boinc/projects/cpdnboinc_dev/html/user/'
	elif project=='CPDN_ALPHA':
		config='/storage/boinc/projects/cpdnboinc_alpha/ancil_batch_user_config.xml'
		out_path='/storage/boinc/projects/cpdnboinc_alpha/html/user/'
        else:
                print "Failed to get project", project
	
	ParseConfig(config) 
	batch_space('upload2')
	print "Finished!"

main()

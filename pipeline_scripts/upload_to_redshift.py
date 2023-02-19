import boto3
import configparser
import matplotlib.pyplot as plt
import pandas as pd
from time import time
import psycopg2
from pipeline_scripts.create_tables import *

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
KEY=config.get("AWS","KEY")
SECRET=config.get("AWS","KEY")

DWH_DB= config.get("DWH","DWH_DB")
DWH_DB_USER= config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD= config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH","DWH_PORT")

DWH_ENDPOINT='dwhcluster.cqikujhmbb8z.us-west-2.redshift.amazonaws.com' 
    
DWH_ROLE_ARN='arn:aws:iam::095458635015:role/dwhRole'

print("Uploading data to Redshift")

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )

sampleDbBucket =  s3.Bucket("my-kanika-009")

conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)

conn = psycopg2.connect(conn_string)
curr = conn.cursor()

print("Creating Tables")

# Create Database
try:
    [
        curr.execute(query) for query in CREATE_TABLES
    ]
except Exception as e:
    print(str(e))

print("Uploading data")
# Upload data
try:
    [
        curr.execute(query) for query in INSERT_QUERIES
    ]
except Exception as e:
    print(str(e))

print("Upload Complete")
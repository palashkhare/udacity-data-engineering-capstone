import boto3
import configparser
import matplotlib.pyplot as plt
import pandas as pd
from time import time
import psycopg2

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
KEY=config.get("AWS","KEY")
SECRET=config.get("AWS","SECRET")
REGION=config.get("AWS","REGION")

DWH_DB= config.get("DWH","DWH_DB")
DWH_DB_USER= config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD= config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH","DWH_PORT")

DWH_ENDPOINT='dwhcluster.commxesjtlrg.us-east-1.redshift.amazonaws.com' 
BUCKET = "my-kanika-009"
DWH_ROLE_ARN='arn:aws:iam::907682692050:role/dwhRole'

print("Uploading data to Redshift")

s3 = boto3.resource('s3',
                       region_name=REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )

sampleDbBucket =  s3.Bucket(BUCKET)

conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)

conn = psycopg2.connect(conn_string)
curr = conn.cursor()

def validate_no_duplicates_immigration():
    total_cnt = "SELECT count(*) FROM IMMIGRATION"
    unique_cnt = "SELECT distinct count(*) FROM IMMIGRATION"

    total_cnt = curr.execute(total_cnt).fetchone()
    unique_cnt = curr.execute(unique_cnt).fetchone()

    assert total_cnt == unique_cnt

def validate_no_duplicates_city():
    total_cnt = "SELECT count(*) FROM CITY"
    unique_cnt = "SELECT distinct count(*) FROM CITY"

    total_cnt = curr.execute(total_cnt).fetchone()
    unique_cnt = curr.execute(unique_cnt).fetchone()

    assert total_cnt == unique_cnt

def validate_no_duplicates_airport():
    total_cnt = "SELECT count(*) FROM AIRPORT"
    unique_cnt = "SELECT distinct count(*) FROM AIRPORT"

    total_cnt = curr.execute(total_cnt).fetchone()
    unique_cnt = curr.execute(unique_cnt).fetchone()

    assert total_cnt == unique_cnt

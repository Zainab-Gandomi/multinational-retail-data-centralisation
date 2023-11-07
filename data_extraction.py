import csv             # for extract data from csv file
import requests        # for extract data from AIP
import boto3           # for extract data from AWS S3 
import pandas as pd
import yaml
import sqlalchemy as db
from data_cleaning import DataCleaning


class DataExtractor:
    def __init__(self):
        pass

    def read_creds(self,yaml_file):
        with open(yaml_file, 'r') as file:
            self.credentials = yaml.safe_load(file)
            return self.credentials

    def init_db_engine(self,creds):
        db_uri = f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = db.create_engine(db_uri)
        conn = engine.connect()
        return conn

    def read_data(self,conn):
        inspector = db.inspect(conn)
        tables = inspector. get_table_names()
        print(tables)
        return tables

    def read_rds_tables(self,con,table):
        df = pd.read_sql_table(table,con)
        # print(df.columns)
        return df
    








'''
    def extract_data_from_csv(self, file_path):
        pass

    def extract_data_from_api(self, url):
        pass

    def extract_data_from_s3_bucket(self, bucket_name, object_key):
        pass
        
'''

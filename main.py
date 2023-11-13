import pandas as pd
from database_utils  import DatabaseConnector 
from data_extraction import DataExtractor  
from data_cleaning   import DataCleaning 
import sqlalchemy


def upload_dim_users():
    de = DataExtractor()
    db = DatabaseConnector()
    dc = DataCleaning()
    # connect to base and get list of frames
    #cred = db.read_db_creds("db_creds.yaml") 
    engine = db.init_db_engine("db_creds.yaml")
    engine.connect()
    tables_list = db.list_db_tables(engine)
    # get clean chosen frame
    df_name = tables_list[1]
    df = dc.clean_user_data(de.read_rds_table( engine, df_name))
    # upload to the db
    #cred_upload = db.read_db_creds("db_creds_upload.yaml") 
    engine = db.init_db_engine("db_creds_upload.yaml")
    engine.connect()
    db.upload_to_db(df,'dim_users',engine)


upload_dim_users()
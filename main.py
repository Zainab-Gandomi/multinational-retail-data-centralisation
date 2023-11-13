import pandas as pd
from database_utils  import DatabaseConnector 
from data_extraction import DataExtractor  
from data_cleaning   import DataCleaning 


def upload_dim_users():
    de = DataExtractor()
    db = DatabaseConnector()
    dc = DataCleaning()
    # connect to base and get list of frames 
    engine = db.init_db_engine("db_creds.yaml")
    engine.connect()
    tables_list = db.list_db_tables(engine)
    # get clean chosen frame
    df_name = tables_list[1]
    df = dc.clean_user_data(de.read_rds_table( engine, df_name))
    # upload datato the db
    engine = db.init_db_engine("db_creds_upload.yaml")
    engine.connect()
    db.upload_to_db(df,'dim_users',engine)

def upload_dim_card_details():
    de = DataExtractor()
    db = DatabaseConnector()
    dc = DataCleaning()
    # connect to link to retrieve data
    pdf_data = de.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
    #clean data
    df = dc.clean_card_data(pdf_data)
    #upload data to the db
    engine = db.init_db_engine("db_creds_upload.yaml")
    engine.connect()
    db.upload_to_db(df,'dim_card_details',engine)


def upload_dim_store_details():
    de = DataExtractor()
    db = DatabaseConnector()
    dc = DataCleaning()  
    # get data
    df = de.retrieve_stores_data()
    print(df[df['store_code']=='WEB-1388012W'])
    df.to_csv('dim_store_details.csv')
    # clean data 
    df = dc.called_clean_store_data(df)
    # upload to db 
    engine = db.init_db_engine("db_creds_upload.yaml")
    engine.connect()
    db.upload_to_db(df,'dim_store_details',engine)

upload_dim_store_details()    












#upload_dim_card_details()
#upload_dim_users()
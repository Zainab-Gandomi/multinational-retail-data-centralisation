import pandas as pd
import yaml
import sqlalchemy as db
import tabula as tb

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
    
    def retrieve_pdf_data(self,link):
        pdf_data = tb.read_odf(link, pages = 'all')
        df_pdf = pd.concat(pdf_data)
        return df_pdf
    




'''
    def extract_data_from_csv(self, file_path):
        pass

    def extract_data_from_api(self, url):
        pass

    def extract_data_from_s3_bucket(self, bucket_name, object_key):
        pass
        
'''

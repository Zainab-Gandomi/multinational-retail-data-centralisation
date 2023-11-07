import psycopg2  # the appropriate database library for database system
import yaml
from yaml.loader import SafeLoader
from sqlalchemy import create_engine, inspect


class DatabaseConnector:

    def __init__(self,file_name = None):
        self.file_name = file_name

    def read_db_creds(self):
        with open(self.file_name) as f:
            data = yaml.load(f, Loader=SafeLoader)
            return data 
        
    def init_db_engine(self):
        data_2 = self.read_db_creds()
        engine = create_engine(f"postgresql+psycopg2://{data_2['RDS_USER']}:{data_2['RDS_PASSWORD']}@{data_2['RDS_HOST']}:{data_2['RDS_PORT']}/{data_2['RDS_DATABASE']}")
        return engine

    def list_db_tables(self):
        inspector = inspect(self.init_db_engine())
        return inspector.get_table_names()
    
    def upload_to_db(self,dataframe,table_name):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = 'P0037979'
        DATABASE = 'Sales_Data'
        PORT = 5432
        engine_2 = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        dataframe.to_sql(table_name,engine_2,if_exists = 'replace')



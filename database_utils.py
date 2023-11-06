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
    


    

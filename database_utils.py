import pandas as pd
import yaml
from sqlalchemy import create_engine, inspect


class DatabaseConnector:


    def __init__(self):
        pass

    def read_db_creds(self,file_name):
        with open(file_name, 'r') as f:
            data = yaml.safe_load(f)
            return data 
        
    def init_db_engine(self, file_name):
        data_2 = self.read_db_creds(file_name)
        engine = create_engine(f"postgresql+psycopg2://{data_2['RDS_USER']}:{data_2['RDS_PASSWORD']}@{data_2['RDS_HOST']}:{data_2['RDS_PORT']}/{data_2['RDS_DATABASE']}")
        engine.connect()
        return engine



    def list_db_tables(self,engine):
        inspector = inspect(engine)
        return inspector.get_table_names()
        
    def upload_to_db(self,df,name,engine):
        df.to_sql(name, engine, if_exists='replace', index = False)
        print(df)

if __name__ == '__main__':
    db = DatabaseConnector()
    engine = db.init_db_engine()
    engine.connect()
    print("Hi") 
    print(engine)
    tables_list = db.list_db_tables(engine)
    print(tables_list)
    with engine.begin() as conn:
        table = pd.read_sql_table(tables_list[1], con=conn)
    print(table)




'''


class DatabaseConnector:

    def __init__(self,file_name):
        self.file_name = file_name

    def read_db_creds(self):
        with open(self.file_name, 'r') as f:
            data = yaml.safe_load(f)
            return data 
        
    def init_db_engine(self):
        data_2 = self.read_db_creds()
        engine = create_engine(f"postgresql+psycopg2://{data_2['RDS_USER']}:{data_2['RDS_PASSWORD']}@{data_2['RDS_HOST']}:{data_2['RDS_PORT']}/{data_2['RDS_DATABASE']}")
        engine.connect()
        return engine

    def list_db_tables(self):
        inspector = inspect(self.init_db_engine())
        print(inspector.get_table_names())

    def upload_to_db(self, df, table_name,engine):
        df.to_sql(table_name, engine , if_exists='replace')


#file_name_yaml = DatabaseConnector('db_creds.yaml')
#file_name_yaml.read_db_creds()
#file_name_yaml.list_db_tables()

#['legacy_store_details', 'legacy_users', 'orders_table']

'''

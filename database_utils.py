import yaml
from sqlalchemy import create_engine, inspect

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


#file_name_yaml = DatabaseConnector('db_creds.yaml')
#file_name_yaml.read_db_creds()
#file_name_yaml.list_db_tables()

#['legacy_store_details', 'legacy_users', 'orders_table']

    def upload_to_db(self, df, table_name):


        engine = create_engine(f"{database_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}")
        """
        df (pd.DataFrame): The DataFrame to upload.
        table_name (str): The name of the destination table in the database.
        """
        try:
            df.to_sql(table_name, con= engine, if_exists='replace', index=False)
            print(f"Data uploaded to '{table_name}' table in PostgreSQL successfully.")
            return True
        except Exception as e:
            print(f"Error uploading data to '{table_name}' table in PostgreSQL: {str(e)}")
            return False

# Create an instance of DatabaCleaning for upload df
init_data = .DataCleaning('legacy_users')
df = init_data.clean_user_data()

# Create an instance of DatabaseConnector
if __name__ ==  __main__ :
    init_data_for_upload = DatabaseConnector('db_creds.yaml')
    # Assuming you have a DataFrame named 'cleaned_data' and you want to upload it to 'dim_users' table
    upload_successful = init_data_for_upload.upload_to_db(df, 'dim_users')

if upload_successful:
    print("Data upload to PostgreSQL was successful.")
else:
    print("Data upload to PostgreSQL failed.")



'''

    
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

'''

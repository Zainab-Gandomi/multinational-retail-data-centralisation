import pandas as pd
import database_utils as du

init_data = du.DatabaseConnector('db_creds.yaml')
db_read = init_data.read_db_creds()
db_engine = init_data.init_db_engine()

class DataExtractor:
    def __init__(self, table_name):
        self.table_name = table_name

    def read_rds_table(self):
        user_data_df = pd.read_sql_table(self.table_name, db_engine)
        df_col = user_data_df.columns
        column_names_list = list(df_col)
        print(column_names_list)
        return user_data_df  

   
db_connector = DataExtractor('legacy_users')

df = db_connector.read_rds_table()
print(df)





'''
import tabula as tb
import requests

    def retrieve_pdf_data(self,link):
        pdf_data = tb.read_odf(link, pages = 'all')
        df_pdf = pd.concat(pdf_data)
        return df_pdf
    
    def list_number_of_stores(self,endpoint,dictionary):
        r = requests.get(endpoint,headers = dictionary)
        output = r.json()
        return output['number_stores']

    def retrieve_stores_data(self):
        list_of_frames = []
        store_number   = self.list_number_of_stores()
        for _ in range(store_number):
            api_url_base = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{_}'
            response = requests.get(
                                    api_url_base,
                                    headers=self.API_key()
                                    )
            list_of_frames.append( pd.json_normalize(response.json()))
        return pd.concat(list_of_frames)

'''
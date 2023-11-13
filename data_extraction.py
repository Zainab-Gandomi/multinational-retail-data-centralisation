import pandas as pd
import tabula
import requests


class DataExtractor:

    def __init__(self):
        pass

    def read_rds_table(self,engine,table_name):
        with engine.begin() as conn:
            return pd.read_sql_table(table_name, con=conn)
            #end of task 3

    def retrieve_pdf_data(self,link):
        return pd.concat(tabula.read_pdf(link, pages='all'))            

    def API_key(self):
        return  {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

    def list_number_of_stores(self):
        api_url_base = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        response = requests.get(
                                api_url_base,
                                headers=self.API_key()
                                )
        return response.json()['number_stores']

    def retrieve_stores_data(self):
        list_of_frames = []
        store_number  = self.list_number_of_stores()
        for _ in range(store_number):
            api_url_base = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{_}'
            response = requests.get(
                                    api_url_base,
                                    headers=self.API_key()
                                    )
            list_of_frames.append( pd.json_normalize(response.json()))
        return pd.concat(list_of_frames)



'''
import pandas as pd
import database_utils as du

#init_data = du.DatabaseConnector('db_creds.yaml')
#db_read = init_data.read_db_creds()
#db_engine = init_data.init_db_engine()

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

#test the retrieve_pdf_data method
init_data = DataExtractor()
df_pdf = init_data.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
print (df_pdf)

'''
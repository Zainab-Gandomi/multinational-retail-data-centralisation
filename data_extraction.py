import pandas as pd
import tabula

class DataExtractor:

    def __init__(self):
        pass

    def read_rds_table(self,engine,table_name):
        with engine.begin() as conn:
            return pd.read_sql_table(table_name, con=conn)
            #end of task 3

    def retrieve_pdf_data(self,link):
        return pd.concat(tabula.read_pdf(link, pages='all'))            


init_data = DataExtractor()
df_pdf = init_data.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
print (df_pdf)

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


'''
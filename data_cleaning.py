import pandas as pd  # appropriate libraries for data cleaning

class DataCleaning:


   def clean_user_data(self,df):
        df = self.clean_invalid_date(df,'date_of_birth')
        df = self.clean_invalid_date(df,'join_date')        
        df = self.clean_NaNs_Nulls_misses(df)
        df.drop(columns='1',inplace=True)
        return df
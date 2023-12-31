import pandas as pd
import numpy as np
import re

class DataCleaning:
    
    def clean_user_data(self,df):
        df = self.clean_invalid_date(df,'date_of_birth')
        df = self.clean_invalid_date(df,'join_date') 
        df = self.clean_NaNs_Nulls_misses(df) 
        df.drop(columns='index',inplace=True)
        return df
    
    def clean_card_data(self,df):
        df['card_number'] = df['card_number'].apply(str)
        df['card_number'] = df['card_number'].replace("?", "")
        df = self.clean_invalid_date(df,'date_payment_confirmed')   
        df.dropna(how="any", inplace =True)
        return df

    def clean_invalid_date(self,df,column_name):
        df[column_name] = pd.to_datetime(df[column_name], format='%Y-%m-%d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%Y %B %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%B %Y %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
        df.dropna(subset = column_name,how='any',inplace= True)
        return df


    def called_clean_store_data(self,df):
        #df.drop(columns='lat',inplace=True)
        df =  self.clean_invalid_date(df,'opening_date')                     
        df['staff_numbers'] =  pd.to_numeric( df['staff_numbers'].apply(self.remove_char_from_string),errors='coerce', downcast="integer") 
        df.dropna(subset = ['staff_numbers'],how='any',inplace= True)
        return df

    def remove_char_from_string(self,value):
        return re.sub(r'\D', '',value)
    

    def convert_product_weights(self,df,column_name):
        df[column_name] = df[column_name].apply(self.get_grams)
        return df

    def get_grams(self,value):
        value = str(value)
        value = value.replace(' .','')
        if value.endswith('kg'):
            value = value.replace('kg','')
            value = self.check_math_operation(value)
            return 1000*float(value) if self.isfloat(value) else np.nan
        elif value.endswith('g'):   
            value = value.replace('g','')
            value = self.check_math_operation(value)
            return float(value) if self.isfloat(value) else np.nan
        elif value.endswith('ml'):   
            value = value.replace('ml','')
            value = self.check_math_operation(value)
            return float(value) if self.isfloat(value) else np.nan
        elif value.endswith('l'):   
            value = value.replace('l','')
            value = self.check_math_operation(value)
            return 1000*float(value) if self.isfloat(value) else np.nan
        elif value.endswith('oz'):   
            value = value.replace('oz','')
            value = self.check_math_operation(value)
            return 28.3495*float(value) if self.isfloat(value) else np.nan
        else:
            np.nan

    def check_math_operation(self,value):
        if 'x' in value:
            value.replace(' ','')
            lis_factors = value.split('x')
            return str(float(lis_factors[0])*float(lis_factors[1]))
        return value

    def isfloat(self,num):
        try:
            float(num)
            return True
        except ValueError:
            return False

    def clean_products_data(self,df):
        df = self.clean_invalid_date(df,'date_added')
        #df.dropna(how='any',inplace= True)
        df.reset_index(inplace=True)       
        return df

    def clean_order_data(self,df):
        df.drop(columns='1',inplace=True)
        df.drop(columns='first_name',inplace=True)
        df.drop(columns='last_name',inplace=True)
        df.drop(columns='level_0',inplace=True)
        df['card_number'] = df['card_number'].apply(self.isDigits)
        df.dropna(how='any',inplace= True)
        return df

    def isDigits(self,num):
        return str(num) if str(num).isdigit() else np.nan
    
    def clean_date_time(self,df):
        df['month']         =  pd.to_numeric( df['month'],errors='coerce', downcast="integer")
        df['year']          =  pd.to_numeric( df['year'], errors='coerce', downcast="integer")
        df['day']           =  pd.to_numeric( df['day'], errors='coerce', downcast="integer")
        df['timestamp']     =  pd.to_datetime(df['timestamp'], format='%H:%M:%S', errors='coerce')
        df.dropna(how='any',inplace= True)
        df.reset_index(inplace=True)       
        return df
    
if __name__ == '__main__':  

    dc = DataCleaning()

    print(str(dc.get_grams('1kg')))
    print(str(dc.get_grams('1g')))
    print(str(dc.get_grams('1l')))
    print(str(dc.get_grams('1ml')))
    print('l1'.isdigit())
    print(str(dc.get_grams('l1ml')))   



'''
import pandas as pd
import database_utils as du

init_data = du.DatabaseConnector('db_creds.yaml')
db_read = init_data.read_db_creds()
db_engine = init_data.init_db_engine()

class DataCleaning:
    def __init__(self, table_name):
        self.table_name = table_name

    def check_null_values(self):                 # create look out for null values
        df = pd.read_sql_table(self.table_name, db_engine)
        null_counts = df.isnull().sum()
        print("NULL Counts for Each Column:")
        print(null_counts)

    def check_data_type(self):                      # Check data types of each column 
        df = pd.read_sql_table(self.table_name, db_engine)  
        data_types = df.dtypes
        print("Data Types:")
        print(data_types)

    def check_numeric_summery(self):                # Check summary statistics for numeric columns
        df = pd.read_sql_table(self.table_name, db_engine) 
        numeric_summary = df.describe()
        print("Numeric Summary Statistics:")
        print(numeric_summary)
    
    def clean_user_data(self):                   # clean date format, drop row with null value, drop the second column
        df = pd.read_sql_table(self.table_name, db_engine)
        df = self.clean_invalid_date(df,'date_of_birth')
        df = self.clean_invalid_date(df,'join_date')        
        df = df.dropna()
        df = df.drop(columns='index',inplace=True)
        print(df)
    
    def clean_invalid_date(self,df,column_name):
        df[column_name] = pd.to_datetime(df[column_name], format='%Y-%m-%d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%Y %B %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%B %Y %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
        return df


init_data_cleaning = DataCleaning('legacy_users')
cleaning_data = init_data_cleaning.clean_user_data()
#checking_numeric_summery = init_data_cleaning.check_numeric_summery()
#checking_data_type = init_data_cleaning.check_data_type()
#print(cleaning_data)


-----------------------------


   # Identify NULL values
null_values = df.isnull().sum()

# Remove rows with NULL values
cleaned_df = df.dropna()

# Fill NULL values with a specific value
cleaned_df = df.fillna(0) 

# Convert 'date_column' to datetime format
df['date_column'] = pd.to_datetime(df['date_column'], errors='coerce')

# Convert 'numeric_column' to numeric type
df['numeric_column'] = pd.to_numeric(df['numeric_column'], errors='coerce')

# Convert 'string_column' to string type
df['string_column'] = df['string_column'].astype(str)


'''
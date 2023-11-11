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
        return df
    
    def clean_invalid_date(self,df,column_name):
        df[column_name] = pd.to_datetime(df[column_name], format='%Y-%m-%d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%Y %B %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%B %Y %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
        df.dropna(subset = column_name,how='any',inplace= True)
        return df


init_data_cleaning = DataCleaning('legacy_users')
cleaning_data = init_data_cleaning.clean_user_data()
#checking_numeric_summery = init_data_cleaning.check_numeric_summery()
#checking_data_type = init_data_cleaning.check_data_type()
print(cleaning_data.dtypes)


'''
    def clean_invalid_date(self,df, column_name):
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
        return df

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
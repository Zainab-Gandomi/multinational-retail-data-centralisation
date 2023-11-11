import pandas as pd
from database_utils  import DatabaseConnector 
from data_extraction import DataExtractor 
from data_cleaning   import DataCleaning



# Create an instance of DatabaCleaning for upload df
init_data = .DataCleaning('legacy_users')
df = init_data.clean_user_data()
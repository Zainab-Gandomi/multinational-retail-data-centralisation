# multinational-retail-data-centralisation

# Data Centralisation Project

In this project, we create a local PostgreSQL database. We upload data from various sources, process it, create a database schema and run SQL queries. 

Key technologies used: *Postgres, AWS (s3), boto3, rest-API, csv, Python (Pandas)*. 

## Project Utils

1. **Data Extraction**: The "data_extraction.py" module houses methods dedicated to extracting data and storing it in Pandas DataFrames from various sources.

2. **Data Cleaning**: Within "data_cleaning.py," we construct the DataCleaning class responsible for cleansing different tables previously uploaded using the methods defined in "data_extraction.py."

3. **Database Data Upload**: In "database_utils.py," we implement the DatabaseConnector class. This class initializes the database engine based on credentials provided in a ".yml" file, facilitating the process of uploading data into the database.

4. **Main Data Upload**: The "main.py" script incorporates methods designed for directly uploading data into the local database. This script serves as a central hub for executing data upload operations.

## Step by Step Data Processing

We obtain data from **six distinct sources**:

1. The primary source is a remote Postgres database hosted on AWS Cloud. The "order_table" within this database is of paramount interest, containing crucial sales information. To ensure data integrity, we focus on specific fields, including "date_uuid," "user_uuid," "card_number," "store_code," "product_code," and "product_quantity." To establish foreign key relationships in our database, we cleanse these columns of any NaN and missing values. Additionally, we enforce the "product_quantity" field to be an integer.

2. Another remote Postgres database, also located on AWS Cloud, contributes user-related data from the "dim_users" table. Employing similar uploading techniques as the first case, we identify the "user_uuid" field as the primary key.

3. A publicly accessible link in AWS Cloud provides access to the "dim_card_details" data stored as a ".pdf" file. Utilizing the "tabula" package for reading ".pdf" files, we extract information. The primary key is derived from the card number, which we convert into a string to address potential issues. Furthermore, we cleanse the data by removing "?" artifacts.

4. Data from the "dim_product" table is sourced from an AWS S3 bucket using the "boto3" package. The "product code" serves as the primary key. During the process, we convert the "product_price" field to a floating-point number. Additionally, the "weight" field is standardized to grams, accounting for different units like ("kg," "oz," "l," "ml").

5. The Restful API is employed to retrieve "dim_store_details" data through the GET method. The ".json" response is converted into a Pandas DataFrame, with the "store_code" acting as the primary key.

6. Accessing data from the "dim_date_times" source is facilitated through a link. The ".json" response is converted into a Pandas DataFrame, and the primary key is identified as "date_uuid."

#### General Data Cleaning Notes


**Ensuring Data Integrity:**
All data cleaning processes are executed with a focus on the "primary key" field. This means that rows in the table are exclusively removed in cases where duplicates or missing values, such as NaNs, are detected in this critical field. This precaution is essential to mitigate the risk of discrepancies between the "foreign key" in the "orders_table" and the "primary key," ensuring the functionality of the database schema.

**Date Transformation Handling Various Formats:**
The date transformation process accommodates diverse time formats by addressing the issue as follows:


'''

        df[column_name] = pd.to_datetime(df[column_name], format='%Y-%m-%d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%Y %B %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%B %Y %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')

'''

This sequence of operations ensures that the data in the specified column is appropriately converted into datetime format, handling variations in time representations. Following the data cleansing process, additional columns are introduced to provide supplementary information about the data once it is loaded into the database.


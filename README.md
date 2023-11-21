# multinational-retail-data-centralisation

# MILESTONE 1

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



# MILESTONE 2

# Create the database schema

## developthe star-based schema of the database,ensuring that the columns are ofthe correct datatypes.

**order_table table**

Deciding the length for a VARCHAR column in SQL involves considering the maximum length of the values expect to store in that column. The length is specified in terms of characters, so need to estimate the maximum number of characters each value might have.

following SQL syntax is used to change the data types to correspond to those seen in the order table below:

+------------------+--------------------+--------------------+
|   orders_table   | current data type  | required data type |
+------------------+--------------------+--------------------+
| date_uuid        | TEXT               | UUID               |
| user_uuid        | TEXT               | UUID               |
| card_number      | TEXT               | VARCHAR(?)         |
| store_code       | TEXT               | VARCHAR(?)         |
| product_code     | TEXT               | VARCHAR(?)         |
| product_quantity | BIGINT             | SMALLINT           |
+------------------+--------------------+--------------------+

'''

        -- Change data type of date_uuid to UUID
        ALTER TABLE orders_table
        ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

        -- Change data type of user_uuid to UUID
        ALTER TABLE orders_table
        ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

        -- Change data type of card_number to VARCHAR(30)
        ALTER TABLE orders_table
        ALTER COLUMN card_number TYPE VARCHAR(30);

        -- Change data type of store_code to VARCHAR(20)
        ALTER TABLE orders_table
        ALTER COLUMN store_code TYPE VARCHAR(20);

        -- Change data type of product_code to VARCHAR(20)
        ALTER TABLE orders_table
        ALTER COLUMN product_code TYPE VARCHAR(20);

        -- Change data type of product_quantity to SMALLINT
        ALTER TABLE orders_table
        ALTER COLUMN product_quantity TYPE SMALLINT;

'''

**dim_user_table table**


**dim_store_details table**
There are two latitude columns in the store details table. By using SQL, merge one of the columns into the other.


Step 1: Merge lat into latitude
Step 2: Drop the lat column
Step 3: Set the data types for column

'''

        UPDATE dim_store_details
        SET latitude = COALESCE(lat, latitude);

        ALTER TABLE dim_store_details
        DROP COLUMN lat;

        ALTER TABLE dim_store_details
        ALTER COLUMN latitude TYPE float8 USING latitude::double precision;

'''

The rest of column typ in store details table is changed with same method as order table.


**dim_product table**


The product_price column has a £ character which need to remove.

Remove £ character from the product_price column:

'''
        UPDATE dim_products
        SET product_price = REPLACE(product_price, '£', '');

'''

The team that handles the deliveries would like a new human-readable column added for the weight so they can quickly make decisions on delivery weights.New column weight_class which contain human-readable values based on the weight range of the product.


+--------------------------+-------------------+
| weight_class VARCHAR(?)  | weight range(kg)  |
+--------------------------+-------------------+
| Light                    | < 2               |
| Mid_Sized                | >= 2 - < 40       |
| Heavy                    | >= 40 - < 140     |
| Truck_Required           | => 140            |
+----------------------------+-----------------+


Add a new column weight_class based on weight ranges:

'''
        ALTER TABLE dim_products
        ADD COLUMN weight_class VARCHAR(20);

        UPDATE dim_products
        SET weight_class=
        CASE
            WHEN weight < 2 THEN 'Light'
            WHEN weight >= 2 AND weight < 40 THEN 'Mid_Size'
            WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
            WHEN weight >= 140 THEN 'Truck_Required'
            ELSE NULL
        END;

'''

The next step involves renaming the "removed" column and changing its data type to boolean.

'''
        ALTER TABLE dim_products
        ADD COLUMN still_available BOOLEAN;

        SELECT DISTINCT(removed) from dim_products;
            
        UPDATE dim_products
        SET still_available =
            CASE
                WHEN removed = 'Still_avaliable' THEN true
                WHEN removed = 'Removed' THEN false
                ELSE NULL
            END;

'''        
now update the rest of column with following code as example:

'''
        ALTER TABLE dim_products
        ALTER COLUMN uuid TYPE UUID USING uuid::UUID;

'''

**dim_date_time table**

'''
        ALTER TABLE dim_date_time
        ALTER COLUMN "year" TYPE VARCHAR(10); 

        ALTER TABLE dim_date_time
        ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

'''

**dim_card_details table**

'''

        ALTER TABLE dim_card_details
        ALTER COLUMN card_number TYPE VARCHAR(50);

        ALTER TABLE dim_card_details
        ALTER COLUMN expiry_date TYPE VARCHAR(20); 

        ALTER TABLE dim_card_details
        ALTER COLUMN date_payment_confirmed TYPE DATE ;

'''

Add foreign and primary keys in connected tables.

'''

        ALTER TABLE dim_products
            ADD PRIMARY KEY (product_code);
        ALTER TABLE orders_table 
            ADD FOREIGN KEY(product_code) 
            REFERENCES dim_products(product_code);


'''

# Mileston 3

## SQL Queries


As primary and foreign keys are settled and data are clean, writing queries can be started in database.

1. How many stores do the business have and in which countries?

'''

        SELECT country_code, COUNT (*) 
        FROM dim_store_details 
        GROUP BY country_code;


'''

![Screenshot (601)](https://github.com/Zainab-Gandomi/multinational-retail-data-centralisation/assets/79536268/8ca9c441-a47f-4268-8319-660ed02f5855)


2. Which locations have the most stores?

'''

        SELECT locality, 
            COUNT (*) 
        FROM dim_store_details 
        GROUP BY locality	
        ORDER BY COUNT(*) DESC;

'''

![Screenshot (602)](https://github.com/Zainab-Gandomi/multinational-retail-data-centralisation/assets/79536268/e9833b05-e542-4678-ab3f-587c14940608)



3. Which months produce the most sales overall time of records?

'''
        SELECT 	dim_date_times.month, 
        ROUND(sum(orders_table.product_quantity*dim_products.product_price)) AS total_revenue
        FROM orders_table
            JOIN dim_date_times ON  orders_table.date_uuid = dim_date_times.date_uuid
            JOIN dim_products ON  orders_table.product_code = dim_products.product_code
        GROUP BY dim_date_times.month
        ORDER BY sum(orders_table.product_quantity*dim_products.product_price) DESC;


'''


![Screenshot (603)](https://github.com/Zainab-Gandomi/multinational-retail-data-centralisation/assets/79536268/0ae6cfae-863a-48c3-a437-de9c1186e4e3)



4. How many sales are coming from online?

'''

        SELECT COUNT (orders_table.product_quantity) AS numbers_of_sales,
            sum(orders_table.product_quantity) AS product_quantity_count,
            CASE 
                WHEN dim_store_details.store_code = 'WEB-1388012W' then 'Web'
            ELSE 'Offline'
            END AS product_location
        FROM orders_table
            JOIN dim_date_times ON  orders_table.date_uuid = dim_date_times.date_uuid
            JOIN dim_products ON orders_table.product_code = dim_products.product_code
            JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
        GROUP BY product_location
        ORDER BY sum(orders_table.product_quantity) ASC;

'''


![Screenshot (604)](https://github.com/Zainab-Gandomi/multinational-retail-data-centralisation/assets/79536268/5ccb1131-cd6b-484f-8207-9c18e92a5233)



5. What percentage of sales comE through each type of store?

'''

        select 	dim_store_details.store_type, 
                round(sum (orders_table.product_quantity*dim_products.product_price)) as revenue,
                round(sum(100.0*orders_table.product_quantity*dim_products.product_price)/(sum(sum(orders_table.product_quantity*dim_products.product_price)) over ())) AS percentage_total
        from orders_table
            join dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
            join dim_products on  orders_table.product_code = dim_products.product_code
            join dim_store_details on orders_table.store_code = dim_store_details.store_code
        group by dim_store_details.store_type
        ORDER BY percentage_total DESC;

'''


![Screenshot (605)](https://github.com/Zainab-Gandomi/multinational-retail-data-centralisation/assets/79536268/82701d4c-b983-40bc-93ea-544ac3efec36)



6.Which month in each year produced the highest cost of sales?

'''

        select  dim_date_times.year,
                dim_date_times.month, 
                round(sum(orders_table.product_quantity*dim_products.product_price)) as revenue
        from orders_table
            join dim_date_times    on  orders_table.date_uuid    = dim_date_times.date_uuid
            join dim_products      on  orders_table.product_code = dim_products.product_code
            join dim_store_details on orders_table.store_code    = dim_store_details.store_code
        group by 	dim_date_times.month,
                    dim_date_times.year
        ORDER BY    sum(orders_table.product_quantity*dim_products.product_price)  DESC;

'''


![Screenshot (606)](https://github.com/Zainab-Gandomi/multinational-retail-data-centralisation/assets/79536268/d64706c6-477d-45bd-9e2d-96406a2cbb9e)


7.What is our staff headcount?

'''

        select  sum(dim_store_details.staff_numbers) as total_staff_numbers, 
            dim_store_details.country_code
        from dim_store_details
        group by dim_store_details.country_code


''

![Screenshot (607)](https://github.com/Zainab-Gandomi/multinational-retail-data-centralisation/assets/79536268/2fcfa20f-e2d8-41b6-9d64-129bcf2ef381)



8.which german store type is selling the most?

'''

        select  round(count(orders_table.date_uuid)) as sales, 
                dim_store_details.store_type, 
                dim_store_details.country_code
        from orders_table
            join dim_date_times    on orders_table.date_uuid    = dim_date_times.date_uuid
            join dim_products      on orders_table.product_code = dim_products.product_code
            join dim_store_details on orders_table.store_code   = dim_store_details.store_code
        where dim_store_details.country_code = 'DE'
        group by 	dim_store_details.store_type,dim_store_details.country_code


'''


![Screenshot (608)](https://github.com/Zainab-Gandomi/multinational-retail-data-centralisation/assets/79536268/fa523c69-0a5b-4c82-9bee-85e58560f862)



9.How quickly is the company making sales?

'''

        ALTER TABLE dim_date_times
        ADD COLUMN time_diff interval;

        UPDATE dim_date_times
        SET time_diff = x.time_diff
        FROM (
        SELECT timestamp, timestamp - LAG(timestamp) OVER (ORDER BY timestamp) AS time_diff
        FROM dim_date_times
        ) AS x
        WHERE dim_date_times.timestamp = x.timestamp;

'''

After creation of column time difference, task query much more straightforward

'''

        select  dim_date_times.year, 		  
            concat('"hours": ',EXTRACT(hours FROM  avg(dim_date_times.time_diff)),' ',
                '"minutes": ',EXTRACT(minutes FROM  avg(dim_date_times.time_diff)),' ',		  
                '"seconds": ',round(EXTRACT(seconds FROM  avg(dim_date_times.time_diff)),2),' '		  
                ) as actual_time_taken		 		  
        from dim_date_times
        group by dim_date_times.year
        order by avg(dim_date_times.time_diff) desc


'''


![Screenshot (609)](https://github.com/Zainab-Gandomi/multinational-retail-data-centralisation/assets/79536268/6418b5ef-9444-4ca9-ad74-884052a4aead)



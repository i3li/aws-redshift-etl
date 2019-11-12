# Purpose
This database can help data analysts at Sparkify in analyzing songs played by users. Origanaly, the data resides on JSON files which are not efficient for querying information. 

# Database Schema
The database is modeled as a start schema where there is only one fact table which is called **songplays** and four dimenstion tables, **users, songs, artists, and time**.
I marked **user_id, song_id, artist_id and start_time** as sortkeys because I think they will be heavily used as JOINS column. In this case, marking them as sortkeys increase performance according to AWS Redshift Documentation. https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key.html

![](ER.png)

Additinoally, there are two tables **staging_songs** and **staging_events** which are merly used for transforming data from the JSON files.

# ETL Pipeline
The ETL pipeline process extracts data from the JSON files in the S3 bucket and loads it into the staging tables in the Postgres database. The copy command is used for loading the data effeciently from the JSON files into the staging table. For the songs JSON files, the property names of the JSON objects match the column names of the **staging_songs** table; hence there are no need for a JSON path file, the mapping is done automatically. On the contrary، the proerty names of the JSON objects in logs JSON files does not match the column names of the **staging_events** table; hence the need for a JSON path file. After we load the staging tables, we extract relevent columns for each table from the staging tables.
# Files
- `create_table.py`: This is where we create our fact and dimension tables for the star schema as well as the staging tables in Redshift.
- `etl.py`: This is where we load data from S3 into staging tables on Redshift and then process that data into our analytics tables on Redshift.
- `sql_queries.py`: This is where we define our SQL statements, which will be imported into the two other files above.
- `dwh.cfg`: This file includes all configuration attributes.
- `IaC.ipynb`: This file is used for creating clusters in code.

# How to Run the Python Scripts
First we need add our configuration properties in the `dwh.cfg`. The `dwh.cfg` file contains these attributes:
- HOST: The cluster's endpoint
- DB_NAME: The name of the database
- DB_USER: The database user
- DB_PASSWORD: The password of the database user
- DB_PORT: The port used to connect to the database
- ARN: The role's ARN

**All attributes under [AWS] are only used in the `IaC.ipynb` for creating clusters. You don't need them if you created your cluster manually.**

Then, we create the tables by running `create_tables.py`. To do that, we need to install these python packages:
- **psycopg2**: `pip install psycopg2`

Then, we run the script: `python3 create_tables.py`
    
After creating the tables, we load the tables by running `etl.py`: `python3 create_tables.py`
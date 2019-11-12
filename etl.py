import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Execute all the copy quaries defined in the copy_table_queries list in the sql_queries.py file to load data into the staging table.
    
    Parameters:
    cur: The curser of the postgresql database connection
    conn: The postgresql database connection object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Execute all the insert statments defined in the insert_table_queries list in the sql_queries.py file to insert data into the fact and dimenstion tables.
    
    Parameters:
    cur: The curser of the postgresql database connection
    conn: The postgresql database connection object
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """The program entry point."""
    
    # Parse the dwh.cfg file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Load all the data into the staging tables
    load_staging_tables(cur, conn)
    
    # Insert all the data into the fact and dimenstion tables
    insert_tables(cur, conn)

    # Close the connection
    conn.close()


if __name__ == "__main__":
    main()
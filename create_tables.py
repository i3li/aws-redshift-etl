import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Delete all the tables defined in the drop_table_queries list in the sql_queries.py file.
    
    Parameters:
    cur: The curser of the postgresql database connection
    conn: The postgresql database connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create all the tables defined in the create_table_queries list in the sql_queries.py file.
    
    Parameters:
    cur: The curser of the postgresql database connection
    conn: The postgresql database connection object
    """
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    # Close the connection
    conn.close()


if __name__ == "__main__":
    main()
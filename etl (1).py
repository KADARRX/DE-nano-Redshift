import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: This function is responsible for loading staging tables  
    using the queries in `copy_table_queries` list. This  loads data 
    from s3 to staging tables.
    Arguments:
        cur: the cursor object.
        conn: connection to the database.
    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: This function is responsible for loading main tables  
    using the queries in `insert_table_queries` list. This  loads data 
    from staging tables to the 5 main tables.
    Arguments:
        cur: the cursor object.
        conn: connection to the database.
    Returns:
        None
    """    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This function 
    - Reads the config file dwh.cfg    
    - Establishes connection with the sparkify database and gets
    cursor to it.      
    - Calls the function to load staging tables      
    - Inserts the data into 5 main tabled from staging tables    
    - Finally, closes the connection. 
    Arguments:
        None
    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
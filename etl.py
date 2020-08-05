import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Copies data from S3 bucket into staging tables.
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Inserts data from staging tables into final tables for analytics team.
    '''    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Loads all necessary variables from config document.
    Connects to Redshift cluster.
    Executes load_staging_tables() function.
    Executes insert_tables() function.
    Closes connection.
    '''      
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
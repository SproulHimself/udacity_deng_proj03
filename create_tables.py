import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Removes all previously exsisting tables
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Creates all tables for staging and final database
    '''    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Loads all necessary variables from config document.
    Connects to Redshift cluster.
    Executes drop_tables() function.
    Executes create_tables() function.
    Closes connection.
    '''    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".
                            format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
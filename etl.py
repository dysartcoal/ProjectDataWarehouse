import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''Load the Redshift staging table data from the source files located on S3.'''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''Insert the data from the staging tables on Redshift into the fact and dimension tables.'''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''Read the configuration parameters from the configuration file and connect to the Redshift cluster.
    Load the staging data into Redshift and then populate the fact and dimension tables.
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
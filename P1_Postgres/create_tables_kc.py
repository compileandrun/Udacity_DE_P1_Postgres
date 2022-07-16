import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import pandas as pd
import os

def create_database():
    """
    Description:
    	- Drops (if exists) and Creates the sparkify database. 
    	- Establishes connection with the sparkify database and gets
    	cursor to it.  

	Arguments:
		None

    Returns:
    	the connection and cursor to sparkifydb
    """
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=super_awesome_application user=koray_v2")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=koray_v2")
    cur = conn.cursor()
    print("connection created \n")
    return cur, conn


def drop_tables(cur, conn):
    """
    Description:
    	Drops each table using the queries in `drop_table_queries` list.

	Arguments:
        cur: cursor object
        conn: object for the connection to the database

    Returns:
    	None
    """
    for query in drop_table_queries:
        print("Dropping Tables")
        cur.execute(query)
        print("Table dropped:",query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description:
    	Creates each table using the queries in `create_table_queries` list. 

	Arguments:
        cur: cursor object
        conn: object for the connection to the database

    Returns:
    	None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("all tables are created")

def main():
    """
	Description:
		Main function. Runs all the functions and closes the connection at the end.
	
	Arguments: 
		None

	Returns:
        cur: cursor object
        conn: object for the connection to the database
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
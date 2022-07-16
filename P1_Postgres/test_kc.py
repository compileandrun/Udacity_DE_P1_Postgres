import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import datetime


def test_table (cur,conn,table_name): 
	"""
    Description:
        This function queries every table in the database and checks if they are filled or not.

    Arguments:
        cur: cursor object
        conn: object for connection to the database
        table_name: name of the table to be queries

    Returns:
        dataframe with the results
    """
	cur.execute("""select * from %s""" % (table_name))
	conn.commit()
	records = cur.fetchall()
	colnames = [desc[0] for desc in cur.description]
	#https://stackoverflow.com/questions/10252247/how-do-i-get-a-list-of-column-names-from-a-psycopg2-cursor -> cur.description
	df_test = pd.DataFrame(records,columns=colnames)
	return df_test
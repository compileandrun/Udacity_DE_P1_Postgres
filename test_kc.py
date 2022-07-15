import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import datetime

#to test if every table in the database is filled.
def test_table (cur,conn,table_name): 
	cur.execute("""select * from %s""" % (table_name))
	conn.commit()
	records = cur.fetchall()
	colnames = [desc[0] for desc in cur.description]
	#https://stackoverflow.com/questions/10252247/how-do-i-get-a-list-of-column-names-from-a-psycopg2-cursor -> cur.description
	df_test = pd.DataFrame(records,columns=colnames)
	return df_test
The goal of this project is to access and extract data from  S3 Buckets and load them first into a staging table and then to a set of production tables in star schema structure.

sql_queries.py file includes all the sql code to drop, create tables and insert data into them.

create_tables.py file includes the functions to run the functions to drop and create the aforementioned tables.

etl.py file includes the functions to copy data to the staging tables and load the data to the production tables both in Redshift.

aws_dw_project_aws_setup.ipynb file runs the code to check the data in the S3 buckets and set the necessary resources and clients in AWS for the project.

aws_dw_project_db_setup.ipynb file creates the connection to the cluster and runs the functions in the create_tables and etl files to finalize the ETL job.

import credentials as c

def create_connection():
	DB_ENDPOINT = c.host
	DB = 'pagila'
	DB_USER = 'koray_v2'
	DB_PASSWORD = 'password'
	DB_PORT = c.port

	# postgresql://username:password@host:port/database
	conn_string = "postgresql://{}:{}@{}:{}/{}" \
	                        .format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB)

	print(conn_string)
	return conn_string
#done

drop_table_actor = "DROP TABLE IF EXISTS actor CASCADE"
drop_table_address = "DROP TABLE IF EXISTS address CASCADE"
drop_table_category = "DROP TABLE IF EXISTS category CASCADE"
drop_table_city = "DROP TABLE IF EXISTS city CASCADE"
drop_table_country = "DROP TABLE IF EXISTS country CASCADE"
drop_table_customer = "DROP TABLE IF EXISTS customer CASCADE"
drop_table_dimdate = "DROP TABLE IF EXISTS dimdate CASCADE"
drop_table_film = "DROP TABLE IF EXISTS film CASCADE"
drop_table_film_actor = "DROP TABLE IF EXISTS film_actor CASCADE"
drop_table_film_category = "DROP TABLE IF EXISTS film_category CASCADE"
drop_table_inventory = "DROP TABLE IF EXISTS inventory CASCADE"
drop_table_language = "DROP TABLE IF EXISTS language CASCADE"
drop_table_payment = "DROP TABLE IF EXISTS payment CASCADE"
drop_table_rental = "DROP TABLE IF EXISTS rental CASCADE"
drop_table_staff = "DROP TABLE IF EXISTS staff CASCADE"
drop_table_store = "DROP TABLE IF EXISTS store CASCADE"

drop_table_dim_date = "DROP TABLE IF EXISTS dimdate CASCADE"
drop_table_dim_customer = "DROP TABLE IF EXISTS dimcustomer CASCADE"
drop_table_dim_movie = "DROP TABLE IF EXISTS dimmovie CASCADE"
drop_table_dim_store = "DROP TABLE IF EXISTS dimstore CASCADE"
drop_table_fact_sales = "DROP TABLE IF EXISTS factsales CASCADE"


create_dim_date =	"""
	DROP TABLE IF EXISTS dimdate;
	CREATE TABLE dimDate
	(
    date_key int PRIMARY KEY,
    date date NOT NULL,
    year  integer,
    quarter integer,
    month int,
    day int,
    week int,
    is_weekend boolean
	);
	"""

create_dim_customer ="""
CREATE TABLE dimCustomer
(
  customer_key SERIAL PRIMARY KEY,
  customer_id  smallint NOT NULL,
  first_name   varchar(45) NOT NULL,
  last_name    varchar(45) NOT NULL,
  email        varchar(50),
  address      varchar(50) NOT NULL,
  address2     varchar(50),
  district     varchar(20) NOT NULL,
  city         varchar(50) NOT NULL,
  country      varchar(50) NOT NULL,
  postal_code  varchar(10),
  phone        varchar(20) NOT NULL,
  active       smallint NOT NULL,
  create_date  timestamp NOT NULL,
  start_date   date NOT NULL,
  end_date     date NOT NULL
);
"""

create_dim_movie = """
CREATE TABLE dimMovie
(
  movie_key          SERIAL PRIMARY KEY,
  film_id            smallint NOT NULL,
  title              varchar(255) NOT NULL,
  description        text,
  release_year       year,
  language           varchar(20) NOT NULL,
  original_language  varchar(20),
  rental_duration    smallint NOT NULL,
  length             smallint NOT NULL,
  rating             varchar(5) NOT NULL,
  special_features   varchar(60) NOT NULL
);
"""

create_dim_store = """
CREATE TABLE dimStore
(
  store_key           SERIAL PRIMARY KEY,
  store_id            smallint NOT NULL,
  address             varchar(50) NOT NULL,
  address2            varchar(50),
  district            varchar(20) NOT NULL,
  city                varchar(50) NOT NULL,
  country             varchar(50) NOT NULL,
  postal_code         varchar(10),
  manager_first_name  varchar(45) NOT NULL,
  manager_last_name   varchar(45) NOT NULL,
  start_date          date NOT NULL,
  end_date            date NOT NULL
);
"""

create_fact_sales = """
CREATE TABLE factSales
(
    sales_key SERIAL PRIMARY KEY,
    date_key integer REFERENCES dimdate (date_key),
    customer_key int REFERENCES dimcustomer (customer_key),
    movie_key int REFERENCES dimmovie (movie_key),
    store_key int REFERENCES dimstore (store_key),
    sales_amount float
    
);
"""
drop_3n_tables = [drop_table_actor, drop_table_address, drop_table_category, drop_table_city,
				drop_table_country, drop_table_customer, drop_table_film, drop_table_film_actor,
				drop_table_film_category,drop_table_inventory, drop_table_language, drop_table_payment, drop_table_rental
				, drop_table_staff, drop_table_store ]


drop_star_tables = [drop_table_dim_date, drop_table_dim_customer, drop_table_dim_movie, 
					drop_table_dim_store, drop_table_fact_sales]

create_star_tables = [create_dim_date, create_dim_customer, create_dim_movie, create_dim_store, create_fact_sales]




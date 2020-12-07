import mysql.connector

#connect to instance
conn_instance = mysql.connector.connect(
  host="localhost",
  user="root",
  password="root",
)
#connect to database
conn = mysql.connector.connect(
  host="localhost",
  user="root",
  password="root",
  database="blocket_data"
)

#create cursor


def create_database(conn_instance, database_name):
  cursor = conn.cursor()
  cursor.execute("CREATE DATABASE " + database_name)

def delete_database(conn_instance, database_name):
  cursor = conn.cursor()
  cursor.execute("DROP DATABASE " + database_name)

def create_table(conn, table_name, columns):
  cursor = conn.cursor()
  column_string = build_column_string(columns)
  cursor.execute("CREATE TABLE " + table_name + " " + column_string)

def build_column_string(columns):
  column_string = "("
  for column in columns:
    column_string += column["column_name"] + " " + column["data_type"]
    if(column["data_type"] == "VARCHAR"):
      column_string += "(" + str(column["column_lenght"]) + ")"
    column_string += ", "
  column_string = column_string[:-2] + ")"
  return column_string

def drop_table(conn, table_name):
  cursor = conn.cursor()
  cursor.execute("DROP TABLE " + table_name)


database_name = "blocket_data"
table_name = "articles"
columns = [
  {"column_name":"location", "data_type":"VARCHAR", "column_lenght":255, "primary_key":False, "auto_increment":False,"not_null":False,"unique":False},
  {"column_name":"time", "data_type":"DATETIME", "primary_key":False, "auto_increment":False,"not_null":False,"unique":False},
  {"column_name":"top_info", "data_type":"VARCHAR", "column_lenght":255, "primary_key":False, "auto_increment":False,"not_null":False,"unique":False},
  {"column_name":"href", "data_type":"VARCHAR", "column_lenght":255, "primary_key":False, "auto_increment":False,"not_null":False,"unique":False},
  {"column_name":"subject_text", "data_type":"VARCHAR", "column_lenght":255, "primary_key":False, "auto_increment":False,"not_null":False,"unique":False},
  {"column_name":"item_id", "data_type":"INT", "primary_key":False, "auto_increment":False,"not_null":False,"unique":False},
  {"column_name":"price", "data_type":"INT", "primary_key":False, "auto_increment":False,"not_null":False,"unique":False},
  {"column_name":"price_text", "data_type":"VARCHAR", "column_lenght":255, "primary_key":False, "auto_increment":False,"not_null":False,"unique":False}
]

# create_table(conn, table_name, columns)
# drop_table(conn, table_name)
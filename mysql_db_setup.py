import mysql_scripts
import os

connection_string_instance = {
  "host": os.environ.get('BLOCKET_SCRAPER_DB_HOST'),
  "user": os.environ.get('BLOCKET_SCRAPER_DB_USER'),
  "password":os.environ.get('BLOCKET_SCRAPER_DB_PASSWORD')
}

connection_string_database = connection_string_instance | {"database":"blocket_data"}

def create_database(connection_string_instance):
    pass

def create_tables(connection_string_database):
    tables_and_columns = [
    {'articles':[
    {"column_name":"id", "data_type":"INT", "primary_key":True, "auto_increment":True,"not_null":True,"unique":True},
    {"column_name":"location", "data_type":"VARCHAR", "column_lenght":255, "primary_key":False, "auto_increment":False,"not_null":False,"unique":False},
    {"column_name":"time", "data_type":"DATETIME", "primary_key":False, "auto_increment":False,"not_null":False,"unique":False},
    {"column_name":"top_info", "data_type":"VARCHAR", "column_lenght":255, "primary_key":False, "auto_increment":False,"not_null":False,"unique":False},
    {"column_name":"href", "data_type":"VARCHAR", "column_lenght":255, "primary_key":False, "auto_increment":False,"not_null":False,"unique":False},
    {"column_name":"subject_text", "data_type":"VARCHAR", "column_lenght":255, "primary_key":False, "auto_increment":False,"not_null":False,"unique":False},
    {"column_name":"item_id", "data_type":"INT", "primary_key":False, "auto_increment":False,"not_null":False,"unique":False},
    {"column_name":"price", "data_type":"INT", "primary_key":False, "auto_increment":False,"not_null":False,"unique":False},
    {"column_name":"price_text", "data_type":"VARCHAR", "column_lenght":255, "primary_key":False, "auto_increment":False,"not_null":False,"unique":False}
    ]},
    {'scrape_log':[
    {"column_name":"id", "data_type":"INT", "primary_key":True, "auto_increment":True,"not_null":True,"unique":True},
    {"column_name":"time_of_scrape", "data_type":"DATETIME", "primary_key":False, "auto_increment":False,"not_null":False,"unique":False},
    {"column_name":"time_of_first_article", "data_type":"DATETIME", "primary_key":False, "auto_increment":False,"not_null":False,"unique":False},
    {"column_name":"no_of_articles", "data_type":"INT", "primary_key":False, "auto_increment":False,"not_null":False,"unique":False}
    ]},
    ]
    connection = mysql_scripts.create_connection(connection_string_database, 'local_database')
    cursor = mysql_scripts.create_cursor(connection)
    for table in tables_and_columns:
        for key, value in table.items():
            mysql_scripts.create_table(cursor, key, value)
    mysql_scripts.close_cursor(cursor)
    mysql_scripts.close_connection(connection)

create_tables(connection_string_database)
import mysql_scripts
import os
import datetime

connection_string_instance = {
  "host": os.environ.get('BLOCKET_SCRAPER_DB_HOST'),
  "user": os.environ.get('BLOCKET_SCRAPER_DB_USER'),
  "password":os.environ.get('BLOCKET_SCRAPER_DB_PASSWORD')
}

connection_string_database = connection_string_instance | {"database":"blocket_data"}

def create_database(connection_string_instance):
    pass

def create_tables(connection):
    tables_and_columns = [
    {'articles':[
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
    {"column_name":"time_of_scrape", "data_type":"DATETIME", "primary_key":False, "auto_increment":False,"not_null":False,"unique":False},
    {"column_name":"time_of_first_article", "data_type":"DATETIME", "primary_key":False, "auto_increment":False,"not_null":False,"unique":False},
    {"column_name":"no_of_articles", "data_type":"INT", "primary_key":False, "auto_increment":False,"not_null":False,"unique":False}
    ]},
    ]
    cursor = mysql_scripts.create_cursor(connection)
    for table in tables_and_columns:
        for key, value in table.items():
            mysql_scripts.create_table(cursor, key, value)
    mysql_scripts.close_cursor(cursor)

def scrape_history_start_up_date(connection):
    # Desc: adding last scraped date as three days ago to avoid heavy load on inital scrape with new database
    # @Parms no parms
    # @output no output
    # Insert Scrape details to scrape log

    time_of_first_article = datetime.datetime.now() - datetime.timedelta(1)
    time_of_scrape = datetime.datetime.now()
    no_of_articles = 0

    cursor = mysql_scripts.create_cursor(connection)
    temporary_scrape_history_insert(connection, cursor, time_of_scrape, time_of_first_article, no_of_articles)
    mysql_scripts.close_cursor(cursor)

def alter_tables_add_id(connection):
    cursor = mysql_scripts.create_cursor(connection)
    cursor.execute("ALTER TABLE scrape_log ADD id INT UNSIGNED NOT NULL AUTO_INCREMENT, ADD INDEX (id)")
    cursor.execute("ALTER TABLE articles ADD id INT UNSIGNED NOT NULL AUTO_INCREMENT, ADD INDEX (id)")
    connection.commit()
    mysql_scripts.close_cursor(cursor)

def temporary_scrape_history_insert(connection, cursor, time_of_scrape, time_of_first_article, no_of_articles):
    try:
        cursor.execute("INSERT INTO scrape_log2 (time_of_first_article, time_of_scrape, no_of_articles) VALUES (%s, %s, %s)",
                    (time_of_first_article, time_of_scrape, no_of_articles))
        connection.commit()
    except:
        print('failed to insert')

def create_table_and_insert_data(connection_string_database):
    connection = mysql_scripts.create_connection(connection_string_database, 'local_database')
    create_tables(connection)
    alter_tables_add_id(connection)
    scrape_history_start_up_date(connection)
    mysql_scripts.close_connection(connection)


create_table_and_insert_data(connection_string_database)
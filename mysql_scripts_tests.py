import mysql_scripts
import datetime
import os

connection_string_instance = {
  "host": os.environ.get('BLOCKET_SCRAPER_DB_HOST'),
  "user": os.environ.get('BLOCKET_SCRAPER_DB_USER'),
  "password":os.environ.get('BLOCKET_SCRAPER_DB_PASSWORD')
}

connection_string_database = connection_string_instance | {"database":"blocket_data"}

database_name = "blocket_data"
table_name = "articles"

def test_insert_many(connection_string_database):
    time1 = datetime.datetime.now()
    time2 = datetime.datetime.now() - datetime.timedelta(1)
    time3 = datetime.datetime.now() - datetime.timedelta(2)


    records_to_insert = [("Göteborg", time1, "boll", "annons/goteborg/aktiv_panther_lang__84/93336788", "kolla in värsta dealen!", 93336788, 123, "123kr"),
                            ("Malmö", time2, "racket", "annons/malmo/aktiv_panther_lang__84/93336789", "kom och köp!", 93336789, 456, "456kr"),
                            ("Stockholm", time3, "nät", "annons/stockholm/aktiv_panther_lang__84/93336787", "billigt, billigt billigt!", 93336787, 789, "789kr")]

    list_of_columns = ["location", "time", "top_info", "href", "subject_text", "item_id", "price", "price_text"]


    connection = mysql_scripts.create_connection(connection_string_database,"local_database")
    cursor = mysql_scripts.create_cursor(connection)
    mysql_scripts.insert_many_data(cursor, connection, list_of_columns, records_to_insert, table_name)
    # mysql_scripts.drop_table(cursor, table_name)
    # mysql_scripts.create_table(cursor, table_name, columns)
    mysql_scripts.close_cursor(cursor)
    mysql_scripts.close_connection(connection)

def test_delete_data_from_table(connection_string_database):
    table_name = "scrape_log"
    connection = mysql_scripts.create_connection(connection_string_database,"local_database")
    cursor = mysql_scripts.create_cursor(connection)
    mysql_scripts.delete_data(cursor, connection, table_name)
    mysql_scripts.close_cursor(cursor)
    mysql_scripts.close_connection(connection)

def test_insert_single(connection_string_database):
  connection = mysql_scripts.create_connection(connection_string_database, "local_database")
  cursor = mysql_scripts.create_cursor(connection)

  table_name = "scrape_log"
  list_of_columns = ["time_of_scrape", "time_of_scrape", "no_of_articles"]

  time_of_first_article = datetime.datetime(2020,12,9,9,15,00,00)
  time_of_scrape = time_of_first_article = datetime.datetime(2020,12,9,9,15,00,00)
  no_of_articles = 98276
  list_of_values = [time_of_first_article, time_of_scrape, no_of_articles]

  cursor.execute("INSERT INTO scrape_log (time_of_first_article, time_of_scrape, no_of_articles) VALUES (%s, %s, %s)",
               (time_of_first_article, time_of_scrape, '90000'))
  connection.commit()
  # mysql_scripts.insert_data(connection, cursor, table_name, list_of_columns, list_of_values)
  mysql_scripts.close_cursor(cursor)
  mysql_scripts.close_connection(connection)

def test_select_data(connection_string_database):
  connection = mysql_scripts.create_connection(connection_string_database, "local_database")
  cursor = mysql_scripts.create_cursor(connection)
  table_name = "scrape_log"
  condition_dict = None
  selection_columns_list = ['time_of_first_article', 'time_of_scrape', 'no_of_articles']
  data = mysql_scripts.select_data(cursor,connection,selection_columns_list,table_name,condition_dict)
  print(data[-1][0])
  mysql_scripts.close_cursor(cursor)
  mysql_scripts.close_connection(connection)

def check_duplicates(connection_string_database):
  connection = mysql_scripts.create_connection(connection_string_database, "local_database")
  cursor = mysql_scripts.create_cursor(connection)
  table_name = "articles"
  condition_dict = None
  selection_columns_list = ['item_id', 'subject_text']
  data = mysql_scripts.select_data(cursor,connection,selection_columns_list,table_name,condition_dict)
  mysql_scripts.close_cursor(cursor)
  mysql_scripts.close_connection(connection)
  duplicate_list = []
  for item in data:
    counter = 0
    for item_check in data:
      if item[0] == item_check[0]:
        counter += 1
    if counter > 1:
      duplicate_list.append(item)
  print(duplicate_list)

def test_environmental_variables(connection_string):
  print(connection_string["password"])



# test_environmental_variables(connection_string_database)

# check_duplicates(connection_string_database)

# test_select_data(connection_string_database)

# test_delete_data_from_table(connection_string_database)

# test_insert_single(connection_string_database)
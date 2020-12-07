import mysql_scripts
import datetime

connection_string_instance = {
  "host":"localhost",
  "user":"root",
  "password":"root",
}

connection_string_database = {
  "host":"localhost",
  "user":"root",
  "password":"root",
  "database":"blocket_data"
}

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
    mysql_scripts.close_connection(cursor, connection)

def test_delete_data_from_table(connection_string_database):
    table_name = "articles"
    connection = mysql_scripts.create_connection(connection_string_database,"local_database")
    cursor = mysql_scripts.create_cursor(connection)
    mysql_scripts.delete_data(cursor, connection, table_name)
    mysql_scripts.close_connection(cursor, connection)


test_delete_data_from_table(connection_string_database)
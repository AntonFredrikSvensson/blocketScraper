import mysql.connector

def create_connection(connection_string, type_of_connection):
  #@param connection_string as dict
  if type_of_connection == "local_instance":
    connection = mysql.connector.connect(
      host=connection_string['host'],
      user=connection_string['user'],
      password=connection_string['password'],
    )
  elif type_of_connection == "local_database":
    connection = mysql.connector.connect(
      host=connection_string['host'],
      user=connection_string['user'],
      password=connection_string['password'],
      database=connection_string['database']
    )
  return connection

def create_cursor(connection):
  cursor = connection.cursor()
  return cursor

def close_connection(cursor,connection):
    cursor.close()
    connection.close()
    print("MySQL connection is closed")

def create_database(cursor, database_name):
  #@param cursor to sql-instance
  cursor.execute("CREATE DATABASE " + database_name)

def delete_database(cursor, database_name):
  #@param cursor to sql-instance
  cursor.execute("DROP DATABASE " + database_name)

def create_table(cursor, table_name, columns):
  # @param 
  # cursor to database
  # columns format: list of dicts 
  #   dict example: {"column_name":"location", "data_type":"VARCHAR", "column_lenght":255, "primary_key":False, "auto_increment":False,"not_null":False,"unique":False}
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

def drop_table(cursor, table_name):
  cursor.execute("DROP TABLE " + table_name)

def build_column_string_for_insert(list_of_columns):
    #building a string of column names based on a list of column names
    #Example input: ['Name','Flag', 'save_number']
    #Example output: Name, Flag, save_number
    insert_string = ''
    for col in list_of_columns:
        insert_string += col + ', '
    insert_string = insert_string[:len(insert_string)-2]
    return insert_string

def build_values_placeholders(list_of_columns):
    # Format 
    # exmple input: ['Name','Flag', 'save_number']
    # example output: "%s, %s, %s, %s"
    placeholders_string = ""
    for col in list_of_columns:
        placeholders_string += '%s, '
    placeholders_string = placeholders_string[:len(placeholders_string)-2]
    return placeholders_string

def insert_many_data(cursor, connection, list_of_columns, records_to_insert, table_name):
    #Format:
    # records_to_insert = [(4, 'HP Pavilion Power', 1999, '2019-01-11'),
    #                     (5, 'MSI WS75 9TL-496', 5799, '2019-02-27'),
    #                     (6, 'Microsoft Surface', 2330, '2019-07-23')]
    # list_of_columns = ['Name','Flag', 'save_number']

    columns_string = build_column_string_for_insert(list_of_columns)
    values_placeholders = build_values_placeholders(list_of_columns)

    #query_string = """INSERT INTO Laptop (Id, Name, Price, Purchase_date) VALUES (%s, %s, %s, %s) """
    query_string ="""INSERT INTO """ + table_name + """ (""" + columns_string + """) VALUES (""" + values_placeholders + """) """
    #print(query_string)

    try:
        cursor.executemany(query_string, records_to_insert)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into table: {}".format(table_name))
    except mysql.connector.Error as error:
        print("Failed to insert data: {}".format(table_name + '' + str(error)))
        print("Failed to insert data: {}".format(error))

def delete_data(cursor, connection, table_name):
    query_string = """DELETE FROM """ + table_name
    try:
        cursor.execute(query_string)
        print("Data deleted successfully from table: {}".format(table_name))
        connection.commit()
    except mysql.connector.Error as error:
         print("Failed to delete data in MySQL: {}".format(error))
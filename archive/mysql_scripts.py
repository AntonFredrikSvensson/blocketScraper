import mysql.connector
import logging

# setting logging config: time, logginglevel, message
logging.basicConfig(filename='mysql_scripts.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')


def create_connection(connection_string, type_of_connection):
    # @param connection_string as dict
    logging.debug('---start create_connection()---')
    if type_of_connection == "local_instance":
        try:
            connection = mysql.connector.connect(
                host=connection_string['host'],
                user=connection_string['user'],
                password=connection_string['password'],
            )
            logging.info('connection to instance established')
        except:
            logging.warning('connection to instance could not be established')
    elif type_of_connection == "local_database":
        try:
            connection = mysql.connector.connect(
                host=connection_string['host'],
                user=connection_string['user'],
                password=connection_string['password'],
                database=connection_string['database']
            )
            logging.info('connection to database established')
        except:
            logging.warning('connection to database could not be established')
    logging.debug('---end create_connection()---')
    return connection


def create_cursor(connection):
    logging.debug('---start create_cursor()---')
    try:
        cursor = connection.cursor()
        logging.info('cursor created')
    except:
        logging.warning('unble to create cursor')
    logging.debug('---end create_cursor()---')
    return cursor


def close_cursor(cursor):
    logging.debug('---start close_cursor()---')
    try:
        cursor.close()
        logging.info('closed cursor')
    except:
        logging.warning('unable to close cursor')
    logging.debug('---end close_cursor()---')


def close_connection(connection):
    logging.debug('---start close_connection()---')
    connection.close()
    logging.info("database connection is closed")
    logging.debug('---start close_connection()---')


def create_database(cursor, database_name):
    # @param cursor to sql-instance
    try:
        cursor.execute("CREATE DATABASE " + database_name)
        logging.info('database {} created'.format(database_name))
    except:
        logging.warning('unable to create database {}'.format(database_name))


def delete_database(cursor, database_name):
    # @param cursor to sql-instance
    try:
        cursor.execute("DROP DATABASE " + database_name)
        logging.info('database {} deleted'.format(database_name))
    except:
        logging.warning('unable to delete database {}'.format(database_name))


def create_table(cursor, table_name, columns):
    # @param
    # cursor to database
    # columns format: list of dicts
    #   dict example: {"column_name":"location", "data_type":"VARCHAR", "column_lenght":255, "primary_key":False, "auto_increment":False,"not_null":False,"unique":False}
    column_string = build_column_string(columns)
    try:

        cursor.execute("CREATE TABLE IF NOT EXISTS " +
                   table_name + " " + column_string)
        logging.info('table {} created'.format(table_name))
    except:
        logging.warning('unable to create table {}'.format(table_name))


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
    try:
        cursor.execute("DROP TABLE " + table_name)
        logging.info('table {} dropped'.format(table_name))
    except:
        logging.warning('unable to drop table {}'.format(table_name))

def build_column_string_for_insert(list_of_columns):
    # building a string of column names based on a list of column names
    # Example input: ['Name','Flag', 'save_number']
    # Example output: Name, Flag, save_number
    insert_string = ''
    for col in list_of_columns:
        insert_string += col + ', '
    insert_string = insert_string[:len(insert_string)-2]
    return insert_string


def build_values_string_for_insert(list_of_values):
    # Building a string to be used in insert statements based on a list of values
    # Example input: ['AntonLand2','AL2.png',2]
    # Example output: 'AntonLand2', 'AL2.png', 2
    values_string = ''
    for value in list_of_values:
        # print(key)
        # print(columns_and_input[key])
        if value == None:
            input_value = 'NULL'
        elif isinstance(value, (float, int)):
            input_value = str(value)
        elif isinstance(value, str):
            input_value = "'" + str(value) + "'"
        values_string += input_value + ', '
    values_string = values_string[:len(values_string)-2]
    return values_string


def build_values_placeholders(list_of_columns):
    # Format
    # exmple input: ['Name','Flag', 'save_number']
    # example output: "%s, %s, %s, %s"
    placeholders_string = ""
    for col in list_of_columns:
        placeholders_string += '%s, '
    placeholders_string = placeholders_string[:len(placeholders_string)-2]
    return placeholders_string

# TODO Fix this function to work with datetime input


def insert_data(connection, cursor, table_name, list_of_columns, list_of_values):
    # connection_string including database
    # format:
    # list_of_columns = ['Name','Flag', 'save_number']
    # list_of_values = ['AntonLand2','AL2.png',2]

    # combining columns into an insert string that is used in the query
    insert_string = build_column_string_for_insert(list_of_columns)

    values_string = build_values_string_for_insert(list_of_values)
    try:
        query_string = """INSERT INTO {} ({}) VALUES ({}) """.format(
            table_name, insert_string, values_string)
        cursor.execute(query_string)
        connection.commit()
        logging.info("data inserted successfully into table {}".format(table_name))
    except mysql.connector.Error as error:
        logging.warning("Failed to insert data to table {}: {}".format(table_name, error))


def temporary_scrape_history_insert(connection, cursor, time_of_scrape, time_of_first_article, no_of_articles):
    table_name = "scrape_log"
    try:
        cursor.execute("INSERT INTO scrape_log (time_of_first_article, time_of_scrape, no_of_articles) VALUES (%s, %s, %s)",
                    (time_of_first_article, time_of_scrape, no_of_articles))
        connection.commit()
        logging.info('data inserted to table scrape_log')
    except:
        logging.warning('unable to insert data into scrape_log')


def insert_many_data(cursor, connection, list_of_columns, records_to_insert, table_name):
    # Format:
    # records_to_insert = [(4, 'HP Pavilion Power', 1999, '2019-01-11'),
    #                     (5, 'MSI WS75 9TL-496', 5799, '2019-02-27'),
    #                     (6, 'Microsoft Surface', 2330, '2019-07-23')]
    # list_of_columns = ['Name','Flag', 'save_number']

    columns_string = build_column_string_for_insert(list_of_columns)
    values_placeholders = build_values_placeholders(list_of_columns)

    #query_string = """INSERT INTO Laptop (Id, Name, Price, Purchase_date) VALUES (%s, %s, %s, %s) """
    query_string = """INSERT INTO """ + table_name + \
        """ (""" + columns_string + """) VALUES (""" + \
        values_placeholders + """) """
    # print(query_string)

    try:
        cursor.executemany(query_string, records_to_insert)
        connection.commit()
        logging.info("{} Record inserted successfully into table: {}".format(cursor.rowcount, table_name))
    except mysql.connector.Error as error:
        logging.warning("Failed to insert data into table {}: {}".format(table_name, str(error)))


def delete_data(cursor, connection, table_name):
    query_string = """DELETE FROM """ + table_name
    try:
        cursor.execute(query_string)
        logging.info("Data deleted successfully from table: {}".format(table_name))
        connection.commit()
    except mysql.connector.Error as error:
        logging.warning("Failed to delete data from table {}: {}".format(table_name, error))


def field_and_increment_string(field_name, increment):
    # input field_name, 3
    # output filed_name = field_name + 3
    return str(field_name) + ' = ' + str(field_name) + ' + ' + str(increment)


def columns_and_input_string_for_update(columns_and_input):
    # building a string of columns and input for a update satatment from a dictionary with keys and value pairs
    # example input: {'save_number': 1, 'Flag':'Italy.png'}
    # example output: save_number = 1, Flag = 'Italy.png'
    # example None as input: columns_and_input = {'save_number': None}
    # Output: save_number IS NULL
    # example increment as input: {'save_number':increment1}
    #Output: 'savenumber = save_number + 1'
    input_string = ''
    for key in columns_and_input:
        # print(key)
        # print(columns_and_input[key])
        value = columns_and_input[key]
        if value == None:
            input_string = key + ' IS NULL'
            return input_string
        elif isinstance(columns_and_input[key], (float, int)):
            input_value = str(columns_and_input[key])
        elif value[:9] == 'increment':
            field_and_increment = field_and_increment_string(key, value[9:])
            input_value = field_and_increment
        elif isinstance(columns_and_input[key], str):
            input_value = "'" + str(columns_and_input[key]) + "'"
        input_string += key + ' = ' + input_value + ', '
    input_string = input_string[:len(input_string)-2]
    return input_string


def condition_string_from_dict(condition_dict):
    # example input: condition_dict = {'id':2}
    # example output: WHERE id = 2
    # example input multiple values, one key: condition_dict = {'id':[2,3]}
    # example output  WHERE `id`  IN (2,3);
    # example None as input: condition_dict = {'save_number': None}
    # Output: save_number IS NULL

    # chech for multiple items
    no_of_items = len(condition_dict)
    if no_of_items < 2:
        # checking for muliple values
        for key in condition_dict:
            if isinstance(condition_dict[key], list):
                condition_string = 'WHERE ' + key + ' IN ('
                for item in condition_dict[key]:
                    condition_string += str(item) + ','
                condition_string = condition_string[:len(condition_string)-1]
                condition_string += ')'
                return condition_string

        # building condition string for single values
        if condition_dict:
            condition_string = 'WHERE ' + \
                columns_and_input_string_for_update(condition_dict)
            return condition_string
        else:
            return False
    else:
        condition_string = 'WHERE '
        counter = 0
        for key in condition_dict:
            if isinstance(condition_dict[key], list):
                condition_string = 'WHERE ' + key + ' IN ('
                for item in condition_dict[key]:
                    condition_string += str(item) + ','
                condition_string = condition_string[:len(condition_string)-1]
                condition_string += ')'

        # for key,value in condition_dict.items():
        #     if isinstance(condition_dict[value],list):
        #         condition_string += key + ' IN ('
        #         for item in condition_dict[value]:
        #             condition_string += str(item) +','
        #         condition_string = condition_string[:len(condition_string)-1]
        #         condition_string+= ')'
            else:
                temp_dict = {}
                temp_dict[key] = condition_dict[key]
                condition_string += columns_and_input_string_for_update(
                    temp_dict)
            counter += 1
            if counter < no_of_items:
                condition_string += ' AND '

    # print(condition_string)
    return condition_string


def select_data(cursor, connection, selection_columns_list, table_name, condition_dict):
    # @params
    # selection_columns_list
    # Example input: ['Name','Flag', 'save_number']
    # condition_dict#example input: condition_dict = {'id':2}
    # example output: WHERE id = 2
    # example None as input: condition_dict = {'save_number': None}
    # Output: save_number IS NULL
    column_string = build_column_string_for_insert(selection_columns_list)

    if condition_dict:
        # will only work whith one condition currently, the function will return ',' as seperator instead of AND
        condition_string = condition_string_from_dict(condition_dict)
    else:
        condition_string = ''

    try:
        if condition_string:
            MySQLQuery = "SELECT {} FROM {} {}".format(
                column_string, table_name, condition_string)
        else:
            MySQLQuery = "SELECT {} FROM {}".format(column_string, table_name)
        # print(MySQLQuery)
        cursor.execute(MySQLQuery)
        records = cursor.fetchall()
        #records = cursor.fetchone()
        logging.info("Total number of rows in selection is: {}".format(cursor.rowcount))

    except mysql.connector.Error as error:
        logging.warning("Failed to select data from table {}: {}".format(table_name, error))
    finally:
        return records

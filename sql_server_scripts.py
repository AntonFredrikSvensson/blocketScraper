import pyodbc
import os
import datetime
import logging

# setting logging config: time, logginglevel, message
logging.basicConfig(filename='mysql_scripts.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

connection = pyodbc.connect('Driver={SQL Server};'
                      'Server=' + os.environ.get('BLOCKET_SCRAPER_DB_SERVER') +';'
                      'Database=BlocketData;'
                      'User=' + os.environ.get('BLOCKET_SCRAPER_DB_USER') + ';'
                      'Trusted_Connection=yes;')

cursor = connection.cursor()

connection_string_database = {
    "Server": os.environ.get('BLOCKET_SCRAPER_DB_HOST'),
    "User": os.environ.get('BLOCKET_SCRAPER_DB_USER'),
    "Password":os.environ.get('BLOCKET_SCRAPER_DB_PASSWORD'),
    "database":"blocket_data"
    }

def create_connection(connection_string, type_of_connection):
    # @param connection_string as dict
    logging.debug('---start create_connection()---')
    if type_of_connection == "local_instance":
        try:
            # connection = mysql.connector.connect(
            #     host=connection_string['host'],
            #     user=connection_string['user'],
            #     password=connection_string['password'],
            # )
            connection = pyodbc.connect('Driver={SQL Server};'
                      'Server=' + connection_string['host'] +';'
                      'Database=BlocketData;'
                      'User=' + connection_string['user'] + ';'
                      'Trusted_Connection=yes;')
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



#https://stackoverflow.com/questions/27683049/elegant-way-to-insert-list-into-odbc-row-using-pyodbc




mysql_scripts.create_connection(connection_string_database, "local_database")


get_time_of_last_scrape(connection)


cursor = mysql_scripts.create_cursor(connection)


def get_time_of_last_scrape(connection):
    # Desc: connects to scrape_log, finds latest scrape, returns latest scrape time
    # @Parms connection:my_sql_connection
    # @output time_of_first_article:datetimeobject
    logging.debug('---start get_time_of_last_scrape()---')
    cursor = mysql_scripts.create_cursor(connection)
    table_name = "scrape_log"
    condition_dict = None
    selection_columns_list = ['time_of_first_article', 'time_of_scrape', 'no_of_articles']
    data = mysql_scripts.select_data(cursor,connection,selection_columns_list,table_name,condition_dict)
    mysql_scripts.close_cursor(cursor)
    time_of_first_article = data[-1][0]
    logging.debug('---end get_time_of_last_scrape()---')
    return time_of_first_article
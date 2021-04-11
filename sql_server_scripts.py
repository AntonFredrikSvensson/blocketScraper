import pyodbc
import os
import datetime
import logging

# setting logging config: time, logginglevel, message
logging.basicConfig(filename='mysql_scripts.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

def create_connection(connection_string, type_of_connection):
    # @param connection_string as dict
    logging.debug('---start create_connection()---')
    if type_of_connection == "local_instance":
        try:
            connection = pyodbc.connect('Driver={SQL Server};'
                      'Server=' + connection_string['Server'] +';'
                      'User=' + connection_string['User'] + ';'
                      'Trusted_Connection=yes;')
            logging.info('connection to instance established')
        except:
            logging.warning('connection to instance could not be established')
    elif type_of_connection == "local_database":
        try:
            connection = pyodbc.connect('Driver={SQL Server};'
                      'Server=' + connection_string['Server'] +';'
                      'Database=BlocketData;'
                      'User=' + connection_string['User'] + ';'
                      'Trusted_Connection=yes;')
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

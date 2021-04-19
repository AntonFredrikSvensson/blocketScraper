import pyodbc
import os
import datetime
import logging

# setting logging config: time, logginglevel, message
logging.basicConfig(filename=os.environ.get('BLOCKET_SCRAPER_LOG_PATH') + 'sql_server_scripts.log', 
                    level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s',
                    force=True)

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

def select_last_scrape(cursor,connection):
    try:
        cursor.execute("SELECT TOP 1 [TimeOfFirstArticle] FROM ScrapeLog ORDER BY ScrapeID DESC")
        time_of_first_article = cursor.fetchone()
    except pyodbc.connector.Error as error:
        logging.warning("Failed to select data from table {}: {}".format("ScrapeLog", error))
    return time_of_first_article[0]

def insert_single_scrape_log(cursor, connection, time_of_scrape, time_of_first_article, no_of_articles):
    # Desc: inserting a single entry to the ScrapeLog
    # @Parms time_of_scrape (datetime), time_of_first_article (datetime), no_of_articles(int)
    # @output no output
    # Insert Scrape details to scrape log
    try:
        cursor.execute('''

                    INSERT INTO BlocketData.dbo.ScrapeLog (TimeOfScrape, TimeOfFirstArticle, NoOfArticles)
                    VALUES (?, ?, ?)

                    ''',(time_of_first_article, time_of_scrape, no_of_articles))
        connection.commit()
        logging.info('data inserted to table ScrapeLog')
    except:
        logging.warning('unable to insert data into ScrapeLog')

def insert_many_articles(cursor, connection, articles):
    # Desc: inserting muliple articles to the article table
    # @Parms articles = [(<Location>, <Time>, <TopInfo>, <Href>, <SubjectText>, <ItemID>, <Price>, <PriceText>, <Store>), ]
    # @output no output

    query = '''insert into BlocketData.dbo.article(Location, Time, TopInfo, Href, SubjectText, ItemID, Price, PriceText, Store) 
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)'''

    cursor.fast_executemany = True
    try:
        cursor.executemany(query, articles)
        connection.commit()
        logging.info('data inserted to Article table')
    except pyodbc.Error as e:
        print(e)
        logging.warning('Unable to insert data into Article table. Error message: ' + str(e))
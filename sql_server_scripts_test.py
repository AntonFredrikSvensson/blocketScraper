import pyodbc
import os
import datetime
import logging
import sql_server_scripts

# setting logging config: time, logginglevel, message
logging.basicConfig(filename='sql_server_scripts_test.log', level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s')

connection_string_database = {
    "Server": os.environ.get('BLOCKET_SCRAPER_DB_SERVER'),
    "User": os.environ.get('BLOCKET_SCRAPER_DB_USER'),
    "Password":os.environ.get('BLOCKET_SCRAPER_DB_PASSWORD'),
    "database":"BlocketData"
    }


connection = sql_server_scripts.create_connection(connection_string_database, "local_database")
cursor = sql_server_scripts.create_cursor(connection)

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
    cursor.executemany(query, articles)
    connection.commit()
    cursor.close()
    connection.close()

# articles = [
#     ('Göteborg', datetime.datetime.now(), 'Byggmaterial', 'https://www.blocket.se/annons/skane/grenstall/95144526', 'Grenställ', 95144526, 100, '100kr', False),
#     ('Malmö', datetime.datetime.now(), 'Bil', 'https://www.blocket.se/annons/stockholm/kia_picanto_5_dorrar_1_2_cvvt_automat_gls_86hk/95144530', 'Kia', 95144530, 100000, '100000 kr', True)
#     ]
# insert_many_articles(cursor, connection, articles)
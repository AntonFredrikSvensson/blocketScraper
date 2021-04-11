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
# sql_server_scripts.close_cursor(cursor)
# sql_server_scripts.close_connection(connection)



def select_last_scrape(cursor,connection):
    cursor.execute("SELECT TOP 1 [TimeOfFirstArticle] FROM ScrapeLog ORDER BY ScrapeID DESC")
    time_of_first_article = cursor.fetchone()
    return time_of_first_article[0]

time_of_first_article = select_last_scrape(cursor,connection)
print(time_of_first_article)
# def get_time_of_last_scrape(connection):
#     # Desc: connects to scrape_log, finds latest scrape, returns latest scrape time
#     # @Parms connection:my_sql_connection
#     # @output time_of_first_article:datetimeobject
#     logging.debug('---start get_time_of_last_scrape()---')
#     cursor = sql_server_scripts.create_cursor(connection)
#     table_name = "scrape_log"
#     condition_dict = None
#     selection_columns_list = ['time_of_first_article', 'time_of_scrape', 'no_of_articles']
#     data = sql_server_scripts.select_data(cursor,connection,selection_columns_list,table_name,condition_dict)
#     sql_server_scripts.close_cursor(cursor)
#     time_of_first_article = data[-1][0]
#     logging.debug('---end get_time_of_last_scrape()---')
#     return time_of_first_article
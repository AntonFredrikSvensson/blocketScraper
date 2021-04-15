import pyodbc
import os
import datetime
import sql_server_scripts

connection = pyodbc.connect('Driver={SQL Server};'
                      'Server=' + os.environ.get('BLOCKET_SCRAPER_DB_SERVER') +';'
                      'Database=BlocketData;'
                      'User=' + os.environ.get('BLOCKET_SCRAPER_DB_USER') + ';'
                      'Trusted_Connection=yes;')

cursor = connection.cursor()

def create_table_scrape_log(cursor,connection):
    cursor.execute('''

               CREATE TABLE ScrapeLog
               (
                ScrapeID int not null identity(1,1),
                TimeOfScrape datetime,
                TimeOfFirstArticle datetime,
                NoOfArticles int
               )

               ''')
    connection.commit()

def create_table_article(cursor,connection):
    cursor.execute('''

               CREATE TABLE Article
               (
                ArticleID int not null identity(1,1),
                Location nvarchar(255),
                Time datetime,
                TopInfo nvarchar(255),
                Href nvarchar(255),
                SubjectText nvarchar(255),
                ItemID int,
                Price int,
                PriceText nvarchar(255),
                Store bit
               )

               ''')
    connection.commit()

def scrape_history_start_up_date(cursor, connection):
    # Desc: adding last scraped date as three days ago to avoid heavy load on inital scrape with new database
    # @Parms no parms
    # @output no output
    # Insert Scrape details to scrape log

    time_of_first_article = datetime.datetime.now() - datetime.timedelta(3)
    time_of_scrape = datetime.datetime.now()
    no_of_articles = 0

    sql_server_scripts.insert_single_scrape_log(cursor, connection, time_of_scrape, time_of_first_article, no_of_articles)

def drop_table(cursor,connection):
    cursor.execute('DROP TABLE TestDB.dbo.scrape_log')
    connection.commit()

create_table_article(cursor,connection)
# create_table_scrape_log(cursor, connection)

# scrape_history_start_up_date(cursor,connection)
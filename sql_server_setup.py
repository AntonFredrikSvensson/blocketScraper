import pyodbc
import os
import datetime

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
                PriceText nvarchar(255)
               )

               ''')
    connection.commit()

def scrape_history_start_up_date(cursor, connection):
    # Desc: adding last scraped date as three days ago to avoid heavy load on inital scrape with new database
    # @Parms no parms
    # @output no output
    # Insert Scrape details to scrape log

    TimeOfFirstArticle = datetime.datetime.now() - datetime.timedelta(3)
    TimeOfScrape = datetime.datetime.now()
    NoOfArticles = 0

    cursor.execute('''

                INSERT INTO BlocketData.dbo.ScrapeLog (TimeOfScrape, TimeOfFirstArticle, NoOfArticles)
                VALUES (?, ?, ?)

                ''',(TimeOfFirstArticle, TimeOfScrape, NoOfArticles))
    connection.commit()

def drop_table(cursor,connection):
    cursor.execute('DROP TABLE TestDB.dbo.scrape_log')
    connection.commit()

# create_table_article(cursor,connection)
# create_table_scrape_log(cursor, connection)

# scrape_history_start_up_date(cursor,connection)
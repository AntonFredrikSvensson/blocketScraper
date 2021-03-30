import pyodbc
import os

connection = pyodbc.connect('Driver={SQL Server};'
                      'Server=' + os.environ.get('BLOCKET_SCRAPER_DB_SERVER') +';'
                      'Database=BlocketData;'
                      'User=' + os.environ.get('BLOCKET_SCRAPER_DB_USER') + ';'
                      'Trusted_Connection=yes;')

cursor = connection.cursor()


def create_table_scrape_log(cursor,connection):
    cursor.execute('''

               CREATE TABLE scrape_log
               (
                id int not null identity(1,1),
                time_of_scrape datetime,
                time_of_first_article datetime,
                no_of_articles int
               )

               ''')
    connection.commit()

def create_table_article(cursor,connection):
    cursor.execute('''

               CREATE TABLE article
               (
                id int not null identity(1,1),
                location nvarchar(255),
                time datetime,
                top_info nvarchar(255),
                href nvarchar(255),
                subject_text nvarchar(255),
                item_id int,
                price int,
                price_text nvarchar(255)
               )

               ''')
    connection.commit()

def insert_to_table(cursor,connection):
    cursor.execute('''

                INSERT INTO TestDB.dbo.article (Name, Age, City)
                VALUES
                ('Jade',20,'London'),
                ('Mary',47,'Boston'),
                ('Jon',35,'Paris')  

                ''')
    connection.commit()

def drop_table(cursor,connection):
    cursor.execute('DROP TABLE TestDB.dbo.People')
    connection.commit()

create_table_article(cursor,connection)
# create_table_scrape_log(cursor, connection)
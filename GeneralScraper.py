from bs4 import BeautifulSoup, Tag
import urllib.request
import datetime
import BlocketDateTime
import sql_server_scripts
import os
import logging

# setting logging config: time, logginglevel, message
logging.basicConfig(filename=os.environ.get('BLOCKET_SCRAPER_LOG_PATH') + 'general_scraper.log', 
                    level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s',
                    force=True)

def scrape():
    # Desc: scrapes all blocket articles from the main page, created since the last scrape
    # @Parms no parms
    # @output no output
    # Inserts articles in articles table
    # Insert Scrape details to scrape log

    logging.info('---Start of scrape()---')

    url = 'https://www.blocket.se/annonser/hela_sverige?'
    #create database connection
    connection_string_database = {
    "Server": os.environ.get('BLOCKET_SCRAPER_DB_SERVER'),
    "User": os.environ.get('BLOCKET_SCRAPER_DB_USER'),
    "Password":os.environ.get('BLOCKET_SCRAPER_DB_PASSWORD'),
    "database":"BlocketData"
    }

    connection = sql_server_scripts.create_connection(connection_string_database, "local_database")
    time_of_last_scrape = get_time_of_last_scrape(connection)

    # initial scrape
    sauce = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(sauce,'lxml')
    pages_content = []
    no_of_articles = 0
    # if no scrape has been done will a scrape be done to chech how many pages blocket holds in total
    if(time_of_last_scrape == None):
        no_of_pages = fetch_pages(soup)
        logging.info('no previous scrape')
        logging.info('pages found: {}'.format(no_of_pages))
    # if a previous scrape has been done, a dummy number is set for pages. This is because the loop will stop when the time of
    # the last scrape is reached and all pages will not be iterated anyway
    else:
        no_of_pages = 200000
    for page in range(no_of_pages):
        print(str(page) + ' of ' + str(no_of_pages))
        quit = False
        # a new scrape will need to be done for each page, the url is built using postfix "page=<page_number>"
        if page != 0:
            page_number = page + 2
            postfix = "page=" + str(page_number)
            newurl = url + postfix
            sauce = urllib.request.urlopen(newurl).read()
            soup = BeautifulSoup(sauce,'lxml')
        scraped_page = scrape_page(soup, time_of_last_scrape)
        if page == 0:
            # the first scraped article will be latest created
            time_of_first_article = scraped_page[0][1]
        if scraped_page == None:
            # if the there is no content in scraped page, the time of last scrape has been reached
            # and the loop can stopp
            logging.info('all pages has been scraped')
            break
        if scraped_page[len(scraped_page)-1] == None:
            # if the last item is None, the page contained less than 40 articles. This means that is was the last page
            # the last item is removed and the loop is ended after the articles have been appended to the list
            logging.info('last scraped page')
            scraped_page.pop()
            quit = True
        for item in scraped_page:
            # appending the scraped articles to the list
            pages_content.append(item)
        if quit:
            break
        logging.debug('page: ' + str(page +1))
        # every 250th page is an insert done in the database.
        # this is done to avoid a huge database job at the end of the program
        # and so that some pages will be inserted to the database in case 
        if page != 0 and page % 250 == 0:
            insert_articles_to_database(connection,pages_content)
            logging.info('inserted to articles table')
            no_of_articles += len(pages_content)
            pages_content = []
    #inserting remaining articles after the loop is finished
    insert_articles_to_database(connection,pages_content)
    logging.info('inserted to articles table')
    no_of_articles += len(pages_content)
    time_of_scrape = datetime.datetime.now()
    # inserting to scrape-details log
    insert_to_scrape_log(connection,time_of_scrape, time_of_first_article, no_of_articles)
    logging.info('inserted to scrape_history table')
    logging.info('---end of scrape()---')

def fetch_pages(soup):
    # Desc: finds number of pages from beautiful soup scraped blocket serach main page
    # @Parms soup:string (html-page fetched with beautiful soup)
    # @output max_page_number:int (number of pages)
    logging.debug('---start fetch_pages()---')
    links = soup.findAll('a')
    max_page_number = 0
    for link in links:
        href = link.get('href')
        if href[0:28] == "/annonser/hela_sverige?page=":
            href_split = href.split("=")
            page_number = int(href_split[1])
            if page_number > max_page_number:
                max_page_number = page_number
    logging.debug('---end fetch_pages()---')
    return max_page_number
    
def scrape_page(soup, time_of_last_scrape):
    # Desc: takes a Blocket search html page and returns a list of articles
    # @Parms soup:string (html-page fetched with beautiful soup), time_of_last_scrape:datetimeobject
    # @output articles_content:list

    logging.debug('---start scrape_page()---')
    articles = soup.findAll('article')
    if len(articles)==0:
        return None
    article_list = extract_articles(articles)
    articles_content = extract_article_content(article_list, time_of_last_scrape)
    logging.debug('---end scrape_page()---')
    return articles_content

def extract_articles(articles):
    # Desc: takes list of articles, pares the divs and returns a list of trimmed articles
    # @Parms articles:list 
    # @output article_list:list

    logging.debug('---start extract_articles()---')
    article_list =[]
    for article in articles:
        wrapper_list = []
        for div in article:
            wrapper_list.append(div)
        content_wrapper = wrapper_list[1]
        content_divs = []
        for div in content_wrapper:
            content_divs.append(div)
        article_list.append(content_divs)
    logging.debug('---end extract_articles()---')
    return article_list

def extract_article_content(article_list, time_of_last_scrape):
    # Desc: takes a list of trimmed articles, parses the components for each article, returns list of complete articles
    # @Parms article_list:list (list of trimmed articles), time_of_last_scrape:datetimeobject
    # @output articles_content:list
    logging.debug('---start extract_article_content()---')
    articles_content = []
    for article in article_list:
        location_time_topinfo = extract_location_time_topinfo(article[0])
        subject_wrapper = extract_subject(article[1])
        #params_wrapper = article[2]##TODO dependent on top_info/category
        sales_info_wrapper = extract_price(article[3])
        article_datetime = BlocketDateTime.blocket_datetime_to_datetime(location_time_topinfo[1])
        if article_datetime == None:
            # constructing a string to pass along to log
            error_time_top_info = ""
            for item in location_time_topinfo:
                error_time_top_info += item
            logging.warning('Article missing missing date and time. see blocket_datetime.log for more info. Id: {}, Href: {}, time_top_info: {}'.format(int(subject_wrapper[2]), subject_wrapper[0], error_time_top_info))
            article_datetime = datetime.datetime.now()
        if time_of_last_scrape != None:
            if time_of_last_scrape > article_datetime:
                articles_content.append(None)
                break
        content = (location_time_topinfo[0],    # location
                    article_datetime,           # time
                    location_time_topinfo[2],   # top_info
                    subject_wrapper[0],         # href
                    subject_wrapper[1],         # subject_text
                    int(subject_wrapper[2]),    # item_id
                    sales_info_wrapper[0],      # price
                    sales_info_wrapper[1],      # price_text
                    False)                      # dummy value for Store
        articles_content.append(content)
    logging.debug('---end extract_article_content()---')
    return articles_content


def extract_location_time_topinfo(location_time_wrapper):
    # Desc: takes a list containing location, time and other info. Extrcts location, time and top_info 
    # which is returned in a list
    # @Parms location_time_wrapper:list
    # @output location_time_topinfo:list
    logging.debug('---start extract_location_time_topinfo()---')
    location_time_list = []
    for item in location_time_wrapper:
        location_time_list.append(item)
    time = location_time_list[2].text
    location_topinfo_wrapper = location_time_list[1]
    location_topinfo_list = []
    for item in location_topinfo_wrapper:
        location_topinfo_list.append(item)        
    try:
        location = location_topinfo_list[2].text
    except:
        location = ""
    top_info = location_topinfo_list[0].text
    location_time_topinfo = [location, time, top_info]
    logging.debug('---end extract_location_time_topinfo()---')
    return location_time_topinfo

def extract_subject(subject_wrapper):
    # Desc: takes a list containing href, subject text and other info. Extrcts href, subject_text and item_id
    # and returns it in a list
    # @Parms subject_wrapper:list
    # @output href_subject_id:list
    logging.debug('---start extract_subject()---')
    href = subject_wrapper.h2.a.get("href")
    subject_text = subject_wrapper.h2.a.span.text
    last_slash = href.rfind('/',0,len(href)) ##reverse find
    characters_to_parse = len(href) - last_slash -1
    item_id = href[-characters_to_parse:]
    href_subject_id = [href, subject_text, item_id]
    logging.debug('---end extract_subject()---')
    return href_subject_id

def extract_price(sales_info):
    # Desc: takes a list containing price, price text and other info. Extrcts price and price text 
    # and returns it in a list
    # @Parms subject_wrapper:list
    # @output price_info:list
    logging.debug('---start extract_price()---')
    try:
        price_text = sales_info.div.div.span.div.text
    except:
        #price is not alwyas mentioned in the article
        price_text = ''
        price = None
        price_info = [price, price_text]
        return price_info
    if price_text =='':
        price = None
    else:
        price = price_text.replace("kr", "")
        price = price.replace(" ", "")
        try:
            price = int(price)
        except:
            logging.warning('price could not be converted to int. Price: {}, Price text: {}'.format(price, price_text))
            price = None
    price_info = [price, price_text]
    logging.debug('---end extract_price()---')
    return price_info

def insert_articles_to_database(connection,records_to_insert):
    # Desc: takes a list of complete articles, inserts them into the articles table in the database
    # @Parms connection:my_sql_connection ,records_to_insert:list of lists
    # @output no output
    # inserts records to articles table
    logging.debug('---start insert_articles_to_database()---')
    cursor = sql_server_scripts.create_cursor(connection)
    sql_server_scripts.insert_many_articles(cursor, connection, records_to_insert)
    sql_server_scripts.close_cursor(cursor)
    logging.debug('---end insert_articles_to_database()---')
    

def insert_to_scrape_log(connection, time_of_scrape,time_of_first_article,no_of_articles):
    # Desc: inserts information about the current scrape into the scrape log
    # @Parms connection:my_sql_connection, time_of_scrape:datetimeobject, time_of_first_article:datetimeobject ,no_of_articles:int
    # @output no output
    # inserts to scrape_log table
    logging.debug('---start insert_to_scrape_log()---')
    cursor = sql_server_scripts.create_cursor(connection)
    sql_server_scripts.insert_single_scrape_log(cursor, connection, time_of_scrape, time_of_first_article, no_of_articles)
    sql_server_scripts.close_cursor(cursor)
    logging.debug('---end insert_to_scrape_log()---')


def get_time_of_last_scrape(connection):
    # Desc: connects to scrape_log, finds latest scrape, returns latest scrape time
    # @Parms connection:my_sql_connection
    # @output time_of_first_article:datetimeobject
    logging.debug('---start get_time_of_last_scrape()---')
    cursor = sql_server_scripts.create_cursor(connection)
    time_of_first_article = sql_server_scripts.select_last_scrape(cursor, connection)
    sql_server_scripts.close_cursor(cursor)
    logging.debug('---end get_time_of_last_scrape()---')
    return time_of_first_article

if __name__ == "__main__":
    scrape()
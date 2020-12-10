from bs4 import BeautifulSoup, Tag
import urllib.request
import datetime
import BlocketDateTime
import mysql_scripts

def scrape(url, time_of_last_scrape):
    sauce = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(sauce,'lxml')
    pages_content = []
    if(time_of_last_scrape == None):
        no_of_pages = fetch_pages(soup)
        # no_of_pages = 10 ##TODO remove variable after tests
    else:
        no_of_pages = 200000
    for page in range(no_of_pages):
        quit = False
        if page != 0:
            page_number = page + 2
            postfix = "page=" + str(page_number)
            newurl = url + postfix
            sauce = urllib.request.urlopen(newurl).read()
            soup = BeautifulSoup(sauce,'lxml')
        scraped_page = scrape_page(soup, time_of_last_scrape)
        if scraped_page == None:
            break
        if scraped_page[len(scraped_page)-1] == None:
            scraped_page.pop()
            quit = True
        for item in scraped_page:
            pages_content.append(item)
        if quit:
            break
        print('page: ' + str(page +1))
        if page % 250 == 0:
            insert_to_database(pages_content)
            pages_content = []
            #inserted to database
    # print(pages_content[2][0])
    insert_to_database(pages_content)
    # print(len(pages_content))

def fetch_pages(soup):
    links = soup.findAll('a')
    max_page_number = 0
    for link in links:
        href = link.get('href')
        if href[0:28] == "/annonser/hela_sverige?page=":
            href_split = href.split("=")
            page_number = int(href_split[1])
            if page_number > max_page_number:
                max_page_number = page_number
    return max_page_number
    
def scrape_page(soup, time_of_last_scrape):
    articles = soup.findAll('article')
    if len(articles)==0:
        return None
    article_list = extract_articles(articles)
    articles_content = extract_article_content(article_list, time_of_last_scrape)
    return articles_content

def extract_articles(articles):
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
    return article_list

def extract_article_content(article_list, time_of_last_scrape):
    articles_content = []
    for article in article_list:
        location_time_topinfo = extract_location_time_topinfo(article[0])
        subject_wrapper = extract_subject(article[1])
        #params_wrapper = article[2]##TODO dependent on top_info/category
        sales_info_wrapper = extract_price(article[3])
        article_datetime = BlocketDateTime.blocket_datetime_to_datetime(location_time_topinfo[1])
        if article_datetime == None:
            #TODO errorlog missing date
            article_datetime = datetime.datetime.now()
        if time_of_last_scrape != None:
            if time_of_last_scrape > article_datetime:
                articles_content.append(None)
                break
            # content = {
            #     "location":location_time_topinfo[0],
            #     "time":article_datetime,
            #     "top_info":location_time_topinfo[2],
            #     "href":subject_wrapper[0],
            #     "subject_text":subject_wrapper[1],
            #     "item_id":int(subject_wrapper[2]),
            #     "price":sales_info_wrapper[0],
            #     "price_text":sales_info_wrapper[1]
            # }
        content = (location_time_topinfo[0],article_datetime,location_time_topinfo[2],subject_wrapper[0],subject_wrapper[1],int(subject_wrapper[2]),sales_info_wrapper[0],sales_info_wrapper[1])
        articles_content.append(content)
    return articles_content


def extract_location_time_topinfo(location_time_wrapper):
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
    return location_time_topinfo

def extract_subject(subject_wrapper):
    href = subject_wrapper.h2.a.get("href")
    subject_text = subject_wrapper.h2.a.span.text
    last_slash = href.rfind('/',0,len(href)) ##reverse find
    characters_to_parse = len(href) - last_slash -1
    item_id = href[-characters_to_parse:]
    href_subject_id = [href, subject_text, item_id]
    return href_subject_id

def extract_price(sales_info):
    try:
        price_text = sales_info.div.div.span.div.text
    except:
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
            #TODO add error log message (example price/month '2995/mån')
            price = None
    price_info = [price, price_text]
    return price_info

def insert_to_database(records_to_insert):
    connection_string_database = {
    "host":"localhost",
    "user":"root",
    "password":"root",
    "database":"blocket_data"
    }
    connection = mysql_scripts.create_connection(connection_string_database, "local_database")
    cursor = mysql_scripts.create_cursor(connection)
    list_of_columns = ["location", "time", "top_info", "href", "subject_text", "item_id", "price", "price_text"]
    table_name = "articles"
    mysql_scripts.insert_many_data(cursor, connection, list_of_columns, records_to_insert, table_name)
    

# def test_scrape_single_page(url):
#     sauce = urllib.request.urlopen(url).read()
#     soup = BeautifulSoup(sauce,'lxml')
#     blabla = scrape_page(soup)
#     print(blabla[1]["time"])


url = 'https://www.blocket.se/annonser/hela_sverige?'
# test_scrape_single_page(url)
# time_of_last_scrape = datetime.datetime(2020,12,3,23,50,00)
time_of_last_scrape = None
scrape(url, time_of_last_scrape)
from bs4 import BeautifulSoup, Tag
import urllib.request
import datetime

def Scrape(url):
    sauce = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(sauce,'lxml')
    #sint = ignore_exception(ValueError)(int) #TODO check what sint is used for
    articles = soup.findAll('article')
    article_list = Extract_articles(articles)
    articles_content = Extract_article_content(article_list)
    print(articles_content[0]["location"])

def Extract_articles(articles):
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

def Extract_article_content(article_list):
    articles_content = []
    for article in article_list:
        location_time_topinfo = Extract_location_time_topinfo(article[0])
        subject_wrapper = Extract_subject(article[1])
        #params_wrapper = article[2]##TODO dependent on top_info/category
        sales_info_wrapper = Extract_price(article[3])
        content = {
            "location":location_time_topinfo[0],
            "time":location_time_topinfo[1],
            "top_info":location_time_topinfo[2],
            "href":subject_wrapper[0],
            "subject_text":subject_wrapper[1],
            "item_id":subject_wrapper[2],
            "price":sales_info_wrapper[0],
            "price_text":sales_info_wrapper[1]
        }
        articles_content.append(content)
    return articles_content


def Extract_location_time_topinfo(location_time_wrapper):
    location_time_list = []
    for item in location_time_wrapper:
        location_time_list.append(item)
    time = location_time_list[2].text
    location_topinfo_wrapper = location_time_list[1]
    location_topinfo_list = []
    for item in location_topinfo_wrapper:
        location_topinfo_list.append(item)
    location = location_topinfo_list[2].text
    top_info = location_topinfo_list[0].text
    location_time_topinfo = [location, time, top_info]
    return location_time_topinfo

def Extract_subject(subject_wrapper):
    href = subject_wrapper.h2.a.get("href")
    subject_text = subject_wrapper.h2.a.span.text
    last_slash = href.rfind('/',0,len(href)) ##reverse find
    characters_to_parse = len(href) - last_slash -1
    item_id = href[-characters_to_parse:]
    href_subject_id = [href, subject_text, item_id]
    return href_subject_id

def Extract_price(sales_info):
    price_text = sales_info.div.div.span.div.text
    if price_text =='':
        price = None
    else:
        price = price_text.replace("kr", "")
        price = price.replace(" ", "")
        price = int(price)
    price_info = [price, price_text]
    return price_info

url = 'https://www.blocket.se/annonser/hela_sverige?'
Scrape(url)
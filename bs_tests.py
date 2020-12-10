from bs4 import BeautifulSoup, Tag
import urllib.request
import GeneralScraper

def fetch_pages_test():
    url = 'https://www.blocket.se/annonser/hela_sverige?'
    sauce = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(sauce,'lxml')
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
            
    # np = GeneralScraper.fetch_pages(soup)
    # print(np)

fetch_pages_test()
# st = "/annonser/hela_sverige?page=12482"
# print(st[0:28])
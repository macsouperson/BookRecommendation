import pandas as pd
import numpy as np
import bs4
import requests
import random
from tqdm import tqdm
import concurrent.futures

def getProxy():
    proxy_tokens = ['219.65.73.81',
                    '34.143.143.61',
                    '219.93.101.60',
                    '9.223.187.19'
                    '95.216.148.196'
                    '23.247.136.248'
                    '200.174.198.86']
    
    proxy = {
        'http': random.choice(proxy_tokens)
    }

    return proxy

def getUserAgent():
    user_agents = [
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 13_5_1 like Mac OS X)'
            'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)',
            'Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59',
            'Opera/9.80 (Macintosh; Intel Mac OS X; U; en) Presto/2.2.15 Version/10.00',
            'Opera/9.60 (Windows NT 6.0; U; en) Presto/2.1.1']

    headers = {'User-Agent':random.choice(user_agents)}

    return headers
    
def getPage(url):

    headers = getUserAgent()
    proxy = getProxy()

    try:
        req = requests.get(url, headers=headers, proxies=proxy)
        
    except requests.exceptions.HTTPError as e: 
        print(f'ran into a code {e} error')

    return bs4.BeautifulSoup(req.text, 'html.parser')

def getBooks(url):

    page = getPage(url)
    books = page.find_all('tr',{'itemtype':'http://schema.org/Book'})

    base_url = 'https://www.goodreads.com'
    book_list = []
    
    for i,b in enumerate(books):
        title, author = 'N/A','N/A'
        avg_rating = -1
        num_rating = -1
        book_url ='N/A'
        scores = -1
        votes = -1 
        try:
            title = b.find('a', class_='bookTitle').text.strip()
            author = b.find('a', class_='authorName').text.strip()
        except: print(f"trouble with title or author of entry {i}")
        try:
            avg, rat = b.find('span', class_='minirating').text.split("—")
            avg_num = avg.replace('really liked it',"")

            num_rating = int(rat.split()[0].replace(",",""))
            avg_rating = float(avg_num.split()[0])
        except: print(f"trouble with ratings of entry {i}")
        try:
            book_url = base_url+b.find('a',class_='bookTitle')['href']
            scores = b.find('span', class_='smallText uitext').text.strip().split('and')
            score = int(scores[0].split(": ")[1].strip().replace(",",""))
            votes = int(scores[1].split()[0].replace(",",""))
        except: print(f"trouble with votes or scores of entry {i}")

        book_entry = {'title':title, 
                    'author':author,
                    'average_rating':avg_rating,
                    'num_rating':num_rating,
                    'score':score,
                    'votes':votes,
                    'url':book_url}
        book_list.append(book_entry)
        
    result = pd.DataFrame(book_list)
    return result

def asynchScrape(urls):
    threads = min(30, len(urls))
    book_list = pd.DataFrame()

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        try: 
            result = list(tqdm(executor.map(getBooks, urls), 
                               desc="Sraping GoodReads hehehe"))
        except:
            print("Couldn't access page")

    for r in result:
        book_list = pd.concat([book_list, r], ignore_index=True)

    return book_list

def setupURLS(base_url, num_pages):
    page_numbers = range(2,num_pages)
    url_add = ['?page='+str(p) for p in page_numbers]

    urls = [base_url+u for u in url_add]
    urls.append(base_url)

    return urls
def main():
    # lists = [('https://www.goodreads.com/list/show/1.Best_Books_Ever',723),
    #          ('https://www.goodreads.com/list/show/143500.Best_Books_of_the_Decade_2020_s', 24)
    #          ('https://www.goodreads.com/list/show/264.Books_That_Everyone_Should_Read_At_Least_Once',306),
    #          ('https://www.goodreads.com/list/show/6.Best_Books_of_the_20th_Century', 78),
    #          ('https://www.goodreads.com/list/show/7.Best_Books_of_the_21st_Century',98),
    #          ('https://www.goodreads.com/list/show/19.Best_for_Book_Clubs',99)]
    
    base_url = 'https://www.goodreads.com/list/show/6.Best_Books_of_the_20th_Century'
    n = 78
    urls = setupURLS(base_url, n)

    books_df = asynchScrape(urls=urls)

if __name__ == '__main__':
    main()
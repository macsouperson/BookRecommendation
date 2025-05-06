from GoodReadsScraper import getProxy, getUserAgent, getPage
import pandas as pd
import bs4
import requests
import re
import concurrent.futures
from tqdm import tqdm

def getGenre(page):
    book_info = page.find_all('div', class_='BookPageMetadataSection__genres')
    genre = 'N/A'
    
    for b in book_info:
        try: 
            genre = b.find('span', class_='BookPageMetadataSection__genreButton').text
        except: genre = 'N/A'

    return genre

def getBookInfo(url=''):

    digit_re = re.compile('^[^\d]*(\d+)')
    page = getPage(url)

    genre = getGenre(page)

    book_info = page.find_all('article', class_='ReviewCard')
    review_info = []

    for i,b in enumerate(book_info):
        review = 'N/A'
        num_rating = -1
        text = 'N/A'

        try: 
            reviewer = b.find('div', class_="ReviewerProfile__name").text
        except: 
            reviewer = 'N/A'
        try:
            rating = b.find('div', class_='ShelfStatus').span['aria-label']
            num_rating = int(re.findall(digit_re, rating)[0])
        except: 
            num_rating = -1
        try:
            text = b.find('section', class_='ReviewText__content').text
        except: 
            text = 'N/A'
        
        review = {'book_url':url,'person':reviewer, 'rating':num_rating, 'text':text, 'genre':genre}
        review_info.append(review)
    
    return pd.DataFrame(review_info)

def asynchScrape(urls):
    threads = min(30,len(urls))
    review_list = pd.DataFrame()

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        try:
            result = list(tqdm(executor.map(getBookInfo, urls), 
                               desc="Scraping Reviews"))
        except: 
            print("couldn't scrap book")
    
    for r in result:
        review_list = pd.concat([review_list, r], ignore_index=True)
    
    return review_list

def main():
    urls = pd.read_csv('data/book_urls.csv')['url'].to_list()

    reviews = asynchScrape(urls)
    reviews.to_csv('data/Reviews.csv')

if __name__ == '__main__':
    main()

import pandas as pd
import bs4
import re

def getBooks():

    file = open('page_ex.html')
    page = bs4.BeautifulSoup(file, 
                             'html.parser')
    
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

result = getBooks()
#print(result)
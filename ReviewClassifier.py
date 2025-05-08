# Sentiment Analysis of GoodReads reviews
import pandas as pd
from tqdm import tqdm
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.probability import FreqDist
import nltk.classify.util 
from sklearn.model_selection import train_test_split
import string
from sqlalchemy import create_engine, text
import ssl
from collections import defaultdict
import psycopg2
import concurrent.futures
import hashlib

analyzer = SentimentIntensityAnalyzer()
##data pre-processing = remove non-english reviews, identify the reviews with ratings 
def fixSSL():
    try:
        _create_unverified_https_context = ssl._create_unverified_context
    except AttributeError:
        pass
    else:
        ssl._create_default_https_context = _create_unverified_https_context

    nltk.download('punkt_tab')
    nltk.download('vader_lexicon')

def getPolarityScore(text):

    try: 
        score = analyzer.polarity_scores(text)
        return score['compound']
    except: 
        return None
    
def createID(url):

    try:
        return hashlib.sha256(url.encode('utf-8')).hexdigest()
    except:
        print("didn't make id")

def main():
    tqdm.pandas()

    engine = create_engine('postgresql+psycopg2://omasood:\
    @localhost:8888/goodreads')
    print("connecting to database...")

    statement = '''SELECT review_id, review FROM reviews'''
    vader_scores = []
    temp_df = pd.read_sql_query(statement, engine, chunksize=50000)
    review_ids = []

    for t in temp_df:
        review_ids.extend(t['review_id'])
        reviews = t['review']
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            try: 
                vader_tmp = list(tqdm(executor.map(getPolarityScore, reviews)))
            except: 
                print("oh no")
        vader_scores.extend(vader_tmp)

    table_df = pd.DataFrame({
        'id':review_ids,
        'vader_scores_num':vader_scores})
    
    # table_df = pd.read_csv('data/TempScores.csv')
    with engine.connect() as conn:
        conn.execute(text('DROP TABLE vader;'))
        conn.commit()
        table_df.to_sql(name='vader', con=engine)
        conn.execute(text('''
                        ALTER TABLE reviews ADD COLUMN vader_score_num real;
                        UPDATE reviews set vader_score_num = vader.vader_scores_num
                        FROM vader
                        WHERE vader.id = reviews.review_id;'''))
        conn.commit()
        conn.close()    
        
if __name__ == '__main__':
    main()
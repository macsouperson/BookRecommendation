import pandas as pd
import pyspark.sql
from sqlalchemy import create_engine, text, types
from collections import defaultdict
from numpy.linalg import svd
import numpy as np
from tqdm import tqdm
import pyspark
from pyspark.sql.functions import col, count
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer

def convertScore(text):

    if text == 'positive': return 1
    elif text == 'negative': return -1
    elif text == 'neutral': return 0
    else: return 

# def ALS(R):
#     n_iter = 20
#     alpha = 0.1
#     n_users, n_books = R.shape
#     M = np.isnan(R)

#     u = np.random.rand(n_users, 2)
#     v = np.random.rand(2, n_books)
#     x = (alpha*np.eye(2))

#     for iter in tqdm(range(n_iter), desc="decomposing"):
#         for i in range(n_users):
#             u[i] = np.array((v[:,M[i]]@R[i,M[i]]).T @ np.linalg.inv((v@v.T)+(x)))
#         for j in range(n_books):
#             v[:,j] = np.array((u.T[:,M[:,j]] @ R[M[:,j],j]) @ np.linalg.inv((u.T@u)+(x)))
            
#     return u,v

spark = SparkSession.builder \
    .appName("BookRecommendation") \
    .config("spark.jars", 
            "/Users/omasood/Desktop/PyProjects/BookRecommendation/postgresql-42.7.5.jar") \
    .config("spark.driver.memory", "15g") \
    .getOrCreate()

reviews = spark.read.format('jdbc') \
    .option('url','jdbc:postgresql://localhost:8888/goodreads') \
    .option('dbtable', 'reviews') \
    .option('user','omasood') \
    .option('driver','org.postgresql.Driver').load()

df = reviews['reviewer_id','vader_score_num', 'book_id','book_url']
book_index = StringIndexer(inputCol='book_id', outputCol='bookIndex').setHandleInvalid('keep')
reviwer_index = StringIndexer(inputCol='reviewer_id', outputCol='userIndex').setHandleInvalid('keep')

final_df = book_index.fit(df).transform(df)
final_df = reviwer_index.fit(final_df).transform(final_df)

final_df = final_df.fillna(0, subset='vader_score_num')

final_df = final_df.withColumn('bookIndex', final_df['bookIndex'].cast('integer')) \
    .withColumn('userIndex', final_df['userIndex'].cast('integer'))

user_counts = final_df.groupBy('userIndex').agg(count('*').alias('numReviews'))\
    .orderBy(col('numReviews'),desc=True)
book_counts = final_df.groupBy('bookIndex').agg(count('*')\
    .alias('bookCount')).orderBy(col('bookCount'),desc=True)

final_df = final_df.join(user_counts, on='userIndex', how='inner')
final_df = final_df.join(book_counts,on='bookIndex', how='inner')

final_df = final_df.where(final_df.numReviews>10)
final_df = final_df.where(final_df.bookCount>10)
final_df = final_df['bookIndex', 'userIndex', 'vader_score_num','book_url', 'numReviews', 'bookCount']

als = ALS(userCol='userIndex', itemCol='bookIndex', 
          ratingCol='vader_score_num', regParam=0.1, rank=2, nonnegative=True)
predictions = als.fit(final_df)
item_factors, user_factors = predictions.itemFactors, predictions.userFactors

# item_recs.show()
#ALS(pivot_df)
# reviews_df = pd.read_sql_query('''SELECT reviewer_id, book_id, vader_score_num
#                                FROM reviews;''', 
#                                con=engine, chunksize=5000)
# all_data = pd.DataFrame()
# for r in reviews_df:
#     try: 
#         #table = r.pivot(index='reviewer_id', columns='book_id',values='vader_score')
#         all_data = pd.concat([all_data, r])
#     except: print("problem")

# print("pivoting...")
# pivot_table = all_data.pivot(index='reviewer_id', 
#                     columns='book_id', 
#                     values='vader_score_num').to_numpy()

# print("calculating ALS...")
# ALS(pivot_table)

# books_df = pd.read_sql_query('''
#                                SELECT DISTINCT *
#                                FROM books b
#                                LEFT JOIN (SELECT genre, book_id FROM reviews) r 
#                                ON b.id = r.book_id;
#                                ''', 
#                                engine)
# with engine.connect() as conn:
#     conn.execute()
#     conn.commit()
#     conn.close()

# engine = create_engine('postgresql+psycopg2://omasood:\
#     @localhost:8888/goodreads')
# print("connecting to database...")
from pyspark.sql.functions import col, count
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.feature import StringIndexer
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import seaborn as sns
import matplotlib.pyplot as plt

def main():
    url = 'jdbc:postgresql://localhost:8888/goodreads'
    params = {
        'user':'omasood',
        'driver':'org.postgresql.Driver'
    }
    spark = SparkSession.builder \
    .appName("BookRecommendation") \
    .config("spark.jars", 
            "postgresql-42.7.5.jar") \
    .config("spark.driver.memory", "15g") \
    .getOrCreate()

    review_df = spark.read.format('jdbc') \
        .option('url',url) \
        .option('dbtable', 'review_data_ml') \
        .option('user','omasood') \
        .option('driver','org.postgresql.Driver').load()
    
    predictions = ALSModel.load('bookrecmodel')

    input = 'https://www.goodreads.com/book/show/7235533-the-way-of-kings'

    lookup = review_df['book_url','bookIndex']

    itemfactors = predictions.itemFactors
    df = itemfactors.toPandas()
    features = df['features'].tolist()
    bookindex = df.id

    book = lookup.filter(review_df.book_url == input).toPandas()
    index = int(list(book.iloc[0])[1])
    
    dists = cosine_similarity(X=features)
    dist_df = pd.DataFrame(dists, columns=bookindex, index=bookindex)

    row = dist_df.iloc[index]
    ranked_row = row.sort_values(ascending=False)
    top10books = ranked_row.iloc[0:10]

    urls = []
    for i in top10books.index:
        book = lookup.filter(review_df.bookIndex == i).toPandas()
        url = list(book.iloc[0])[0]
        urls.append(url)
    
    print(urls)
    # top25 = dists[:25,:25]
    # sns.heatmap(top25, annot=False, cmap='crest')
    # plt.show()
    
if __name__ == '__main__':
    main()
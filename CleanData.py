from pyspark.sql.functions import col, count
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.feature import StringIndexer

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

    reviews = spark.read.format('jdbc') \
        .option('url',url) \
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
    final_df = final_df['bookIndex', 'userIndex', 'reviewer_id', 'vader_score_num','book_url', 'numReviews', 'bookCount']

    # print(final_df.count(), len(final_df.columns))
    # name = 'review_data_ml'
    # final_df.write.jdbc(url, name, mode='overwrite', properties=params)

    # als = ALS(userCol='userIndex', itemCol='bookIndex', 
    #         ratingCol='vader_score_num', regParam=0.1, rank=2, nonnegative=True)
    # predictions = als.fit(final_df)
    
    # predictions.write().overwrite().save('bookrecmodel')
    # spark.stop()

if __name__ == '__main__':
    main()

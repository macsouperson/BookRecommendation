from pyspark.sql.functions import col, count
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.feature import StringIndexer
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator

def refineData(original_data):

    url = 'jdbc:postgresql://localhost:8888/goodreads'
    params = {
        'user':'omasood',
        'driver':'org.postgresql.Driver'
    }
    name = 'review_data_ml'

    book_index = StringIndexer(inputCol='book_id', outputCol='bookIndex').setHandleInvalid('keep')
    reviwer_index = StringIndexer(inputCol='reviewer_id', outputCol='userIndex').setHandleInvalid('keep')

    final_df = book_index.fit(original_data).transform(original_data)
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
    final_df.write.jdbc(url, name, mode='overwrite', properties=params)

    return final_df

def buildModel(data):

    train, test = data.randomSplit([0.7,0.3])
    #maxIter, regParam and alpha

    als = ALS(userCol='userIndex', itemCol='bookIndex', 
            ratingCol='vader_score_num', nonnegative=False, coldStartStrategy='drop')
    
    grid = ParamGridBuilder() \
        .addGrid(ALS.maxIter, [15,17,20,22]) \
        .addGrid(ALS.regParam, [0.1,0.2,0.25,0.3,0.5,1]) \
        .addGrid(ALS.alpha, [0.1,0.5,1]) \
        .build()

    evaluator = RegressionEvaluator(metricName='rmse', labelCol='vader_score_num',
                                     predictionCol='prediction')
    cv = CrossValidator(numFolds=5, estimator=als,
                        estimatorParamMaps=grid, evaluator=evaluator, parallelism=4)
    cv_models = cv.fit(train)

    bestmodel = cv_models.bestModel
    predictions = bestmodel.transform(test)
    rmse = evaluator.evaluate(predictions)

    print(f'the best model has an RMSE score of {rmse}')

    bestmodel.write().overwrite().save('bookrecmodel')

    return bestmodel

def main():
    
    url = 'jdbc:postgresql://localhost:8888/goodreads'
    spark = SparkSession.builder \
    .appName("BookRecommendation") \
    .config("spark.jars", 
            "postgresql-42.7.5.jar") \
    .config("spark.driver.memory", "15g") \
    .getOrCreate()

    data = spark.read.format('jdbc') \
        .option('url',url) \
        .option('dbtable', 'review_data_ml') \
        .option('user','omasood') \
        .option('driver','org.postgresql.Driver').load()
    
    #reviews_df = refineData(data)
    model = buildModel(data)

    # name = 'review_data_ml'
    # final_df.write.jdbc(url, name, mode='overwrite', properties=params)
    spark.stop()

if __name__ == '__main__':
    main()

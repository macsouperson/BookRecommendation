from pyspark.sql.functions import col, count
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS, ALSModel
import pyspark
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

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
    userfactors, itemfactors = predictions.userFactors, predictions.itemFactors

    user_df = userfactors.toPandas()
    item_df = itemfactors.toPandas()

    user_mat = np.array([*user_df.features])
    item_mat = np.array([*item_df.features])

    user_indx = np.array([*user_df.id])
    item_indx = np.array([*item_df.id])
    
    R_hat = user_mat@item_mat.T
    R_df = pd.DataFrame(R_hat, index=user_indx, columns=item_indx)
    
    reviews = [0]*21312
    reviews[100] = 1
    reviews[200] = 0.5
    reviews[300] = 0.75
    reviews[400] = -1
    reviews[500] = -0.25

    newuser = {'userIndex':7000,
               'vader_score_num':reviews,
               'bookIndex':item_indx}
    
    #user = pd.DataFrame(newuser)
    user = pyspark.sql.DataFrame(newuser,spark)
    user.show()
    #print(user)
    # guess = predictions.transform(user)
    # print(guess)
    
if __name__ == '__main__':
    main()
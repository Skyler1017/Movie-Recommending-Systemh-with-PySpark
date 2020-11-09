# coding=utf-8
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.ml.feature import StandardScaler
import pandas as pd
import os
os.environ['PYSPARK_PYTHON']='/usr/local/bin/python3'


def CreatSparkContext():
    sparkConf=SparkConf().setAppName("WordCounts").set("spark.ui.showConsoleProgress","true")
    sc=SparkContext(conf=sparkConf)
    print("master=" + sc.master)
    SetPath(sc)
    return(sc)

def SetLogger(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

def SetPath(sc):
    global Path
    if sc.master[0:5]=="local":
        Path="file:///Users/wskyler/PycharmProjects/MovieRecommend/data"
    else:
        Path="hdfs://localhost:9000/"

# x and y are both list of (userID, rating) pairs
def getSimilarity(x, y):
    # Jaccard 距离 TODO 基于评分差距修正
    rating_map = {}
    for user, rating in x:
        rating_map[user] = rating
    common_cnt = 0;
    common_ratio = 0
    for user, rating in y:
        if user in rating_map:
            common_cnt += 1
            diff = abs(rating_map[user] - rating)
            if diff <= 1:
                common_ratio += 1
            elif diff <= 1.5:
                common_ratio += 0.5
    return common_ratio / (len(x) + len(y) - common_cnt)


if __name__=="__main__":
    # 创建spark环境
    sc = CreatSparkContext()
    # 创建spark SQL 环境
    spark = SparkSession(sc)

    # 读入数据
    fileDF = pd.read_csv(Path + "small/ratings.csv")
    ratingDF = spark.createDataFrame(fileDF)
    raw_rating = ratingDF.rdd
    print(raw_rating.first())

    # user_item
    user_item = raw_rating.map(lambda x: (x[0], (x[1], x[2])))\
        .groupByKey()
    print(user_item.first())

    # item_user
    item_user = raw_rating.map(lambda x: (x[1], (x[0], x[2])))\
        .groupByKey()\
        .mapValues(lambda x: sorted(list(x))) # 按 ID 大小排序，便于计算距离
    print(item_user.first())

    # hot movies
    hot_movies = raw_rating.map(lambda x: (x[1], (x[2], 1)))\
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
        .mapValues(lambda x: (x[0]/x[1], x[1]))\
        .sortBy(lambda x: (x[1][1], x[1][0]), False) # 先按看过的人数排序，再按平均得分排序
    print(hot_movies.first())

    # item-item pair
    item_item = item_user.\
        cartesian(item_user)\
        .filter(lambda x: x[0][0] != x[1][0])
    print(item_item.first())

    # calculate distance
    movie_distance = item_item.map(lambda x: (x[0][0], (x[1][0], getSimilarity(x[0][1], x[1][1]))))\
        .groupByKey().mapValues(lambda x: sorted(x, reverse=True))

    # calculate tag similarity
    print(movie_distance.first())


    # recommend
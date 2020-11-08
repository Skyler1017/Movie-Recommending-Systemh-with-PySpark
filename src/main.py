# coding=utf-8
from pyspark import SparkContext
from pyspark import SparkConf
from src.utils.get import get
from src.utils.find import find

from src.utils.getDistance import cosDistance


def CreatSparkContext():
    sparkConf=SparkConf().setAppName("WordCounts").set("spark.ui.showConsoleProgress","true")
    sc=SparkContext(conf=sparkConf)
    print("master=" + sc.master)
    SetLogger(sc)
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
        Path="file:///Users/wskyler/PycharmProjects/MovieRecommend/data/lab2/"
    else:
        Path="hdfs://localhost:9000/"


def prepareData(sc):
    rawUserData = sc.textFile(Path+"ratings.dat")
    rawRatings = rawUserData.map(lambda line:line.split("::")[:2])
    ratingsRDD = rawRatings.map(lambda x:(x[0],x[1]))
    return(ratingsRDD)



def recommend(W, user_item, user_id,k):
    user_see_item = user_item.filter(lambda x:x[0]==user_id).map(lambda x:x[1]).collect()[0]
    sim_item = W.filter(lambda x:find(x[0], user_see_item)).map(lambda x:x[1])
    recommend_item = sim_item.flatMap(lambda x:get(x,k)).reduceByKey(lambda x,y:x+y)
    recommend_item = recommend_item.sortBy(lambda x:x[1],False)
    return recommend_item.take(k)


if __name__=="__main__":
    # if len(sys.argv) != 3:
    #     print("wrong input")
    #     exit(-1)
    sc = CreatSparkContext()
    # 数据处理，得到用户观看电影矩阵以及电影被观看矩阵RDD
    print("prepare data")
    ratingsRDD = prepareData(sc)


    user_item = ratingsRDD.groupByKey().map(lambda x:(x[0],list(x[1])))
    print("got user_item")
    print(user_item.first())


    RDD = ratingsRDD.map(lambda x:(x[1],x[0]))
    item_user = RDD.groupByKey().map(lambda x:(x[0],list(x[1])))
    print("got item_user")
    print(item_user.first())

    print("item_user---item_user")
    item_user = item_user.cartesian(item_user).filter(lambda x:x[0][0]!=x[1][0])
    print(item_user.first())

    print("calculate")
    w = item_user.map(lambda x:(x[0][0], x[1][0], cosDistance(x[0][1], x[1][1])))
    W = w.map(lambda x:(x[0],(x[1],x[2])))
    W = W.groupByKey().map(lambda x:(x[0],list(x[1])))
    print("got similar table")
    print(W.first())
    print("recommending.......")
    user_id = u'200'
    recommend = recommend(W, user_item, user_id, 5)
    print("Result:")
    for r in recommend:
        print(r)



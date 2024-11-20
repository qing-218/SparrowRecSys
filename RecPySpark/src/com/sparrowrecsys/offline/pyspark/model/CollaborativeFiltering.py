from pyspark import SparkConf
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

if __name__ == '__main__':
    # 配置 Spark 环境，设置应用名和运行模式
    conf = SparkConf().setAppName('collaborativeFiltering').setMaster('local')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    #/Users/zhewang/Workspace/SparrowRecSys/src/main/resources/webroot/modeldata
    file_path = 'file:///Users/zhewang/Workspace/SparrowRecSys/src/main/resources'
    ratingResourcesPath = file_path + '/webroot/sampledata/ratings.csv'
    # 读取 ratings.csv 数据并转换列类型
    ratingSamples = spark.read.format('csv').option('header', 'true').load(ratingResourcesPath) \
        .withColumn("userIdInt", F.col("userId").cast(IntegerType())) \
        .withColumn("movieIdInt", F.col("movieId").cast(IntegerType())) \
        .withColumn("ratingFloat", F.col("rating").cast(FloatType()))
     # 将数据集分为训练集 (80%) 和测试集 (20%)
    training, test = ratingSamples.randomSplit((0.8, 0.2))
    # Build the recommendation model using ALS on the training data
    # Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    # 使用 ALS 算法训练推荐模型
    als = ALS(regParam=0.01, maxIter=5, userCol='userIdInt', itemCol='movieIdInt', ratingCol='ratingFloat',
              coldStartStrategy='drop')
    model = als.fit(training)
    # Evaluate the model by computing the RMSE on the test data
     # 使用测试数据对模型进行评估
    predictions = model.transform(test)
    model.itemFactors.show(10, truncate=False)
    model.userFactors.show(10, truncate=False)
    # 计算 RMSE (Root Mean Square Error)
    evaluator = RegressionEvaluator(predictionCol="prediction", labelCol='ratingFloat', metricName='rmse')
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error = {}".format(rmse))
    # Generate top 10 movie recommendations for each user
    # 为每个用户生成前 10 个推荐电影
    userRecs = model.recommendForAllUsers(10)
    # Generate top 10 user recommendations for each movie
    # 为每个电影生成前 10 个推荐用户
    movieRecs = model.recommendForAllItems(10)
    # Generate top 10 movie recommendations for a specified set of users
    # 为指定用户子集生成推荐
    users = ratingSamples.select(als.getUserCol()).distinct().limit(3)
    userSubsetRecs = model.recommendForUserSubset(users, 10)
    # Generate top 10 user recommendations for a specified set of movies
    # 为指定电影子集生成推荐
    movies = ratingSamples.select(als.getItemCol()).distinct().limit(3)
    movieSubSetRecs = model.recommendForItemSubset(movies, 10)
    userRecs.show(5, False)
    movieRecs.show(5, False)
    userSubsetRecs.show(5, False)
    movieSubSetRecs.show(5, False)
    # 设置参数网格进行交叉验证
    paramGrid = ParamGridBuilder().addGrid(als.regParam, [0.01]).build()
    cv = CrossValidator(estimator=als, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=10)
    # 使用交叉验证模型拟合数据
    cvModel = cv.fit(test)
    avgMetrics = cvModel.avgMetrics
    spark.stop()

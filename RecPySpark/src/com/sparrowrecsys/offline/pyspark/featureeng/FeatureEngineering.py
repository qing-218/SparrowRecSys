from pyspark import SparkConf
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, QuantileDiscretizer, MinMaxScaler
from pyspark.ml.linalg import VectorUDT, Vectors
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F


def oneHotEncoderExample(movieSamples):
    """
    对电影 ID 进行 One-Hot 编码。
    输入:
        movieSamples (DataFrame): 包含电影样本的 DataFrame。
    过程:
        1. 将 `movieId` 转换为整数类型。
        2. 使用 OneHotEncoderEstimator 对 `movieIdNumber` 进行编码。
        3. 显示编码后的数据结构和内容。
    """
    samplesWithIdNumber = movieSamples.withColumn("movieIdNumber", F.col("movieId").cast(IntegerType()))
    encoder = OneHotEncoderEstimator(inputCols=["movieIdNumber"], outputCols=['movieIdVector'], dropLast=False)
    oneHotEncoderSamples = encoder.fit(samplesWithIdNumber).transform(samplesWithIdNumber)
    oneHotEncoderSamples.printSchema()
    oneHotEncoderSamples.show(10)


def array2vec(genreIndexes, indexSize):
    """
    将类别索引列表转换为稀疏向量。
    参数:
    - genreIndexes: 包含多个类别索引的列表，例如电影的不同类别。
    - indexSize: 稀疏向量的总维度，通常是类别的总数。
    返回:
    - 返回一个稀疏向量，表示各类别的存在性。
    """
    genreIndexes.sort()
    fill_list = [1.0 for _ in range(len(genreIndexes))]
    return Vectors.sparse(indexSize, genreIndexes, fill_list)


def multiHotEncoderExample(movieSamples):
    """
    对电影的 genres 特征进行 Multi-Hot 编码。
    输入:
        movieSamples (DataFrame): 包含电影样本的 DataFrame。
    过程:
        1. 将 `genres` 字段分解为独立的单个类型。
        2. 使用 StringIndexer 对每个类型进行索引化。
        3. 将每部电影的所有类型索引收集成一个列表。
        4. 将索引列表转化为稀疏向量表示。
    """
    samplesWithGenre = movieSamples.select("movieId", "title", explode(
        split(F.col("genres"), "\\|").cast(ArrayType(StringType()))).alias('genre'))
    genreIndexer = StringIndexer(inputCol="genre", outputCol="genreIndex")
    StringIndexerModel = genreIndexer.fit(samplesWithGenre)
    genreIndexSamples = StringIndexerModel.transform(samplesWithGenre).withColumn("genreIndexInt",
                                                                                  F.col("genreIndex").cast(IntegerType()))
    indexSize = genreIndexSamples.agg(max(F.col("genreIndexInt"))).head()[0] + 1
    processedSamples = genreIndexSamples.groupBy('movieId').agg(
        F.collect_list('genreIndexInt').alias('genreIndexes')).withColumn("indexSize", F.lit(indexSize))
    finalSample = processedSamples.withColumn("vector",
                                              udf(array2vec, VectorUDT())(F.col("genreIndexes"), F.col("indexSize")))
    finalSample.printSchema()
    finalSample.show(10)


def ratingFeatures(ratingSamples):
    """
    对评分数据进行特征工程，包括统计计算、分桶和归一化。
    输入:
        ratingSamples (DataFrame): 包含评分样本的 DataFrame。
    过程:
        1. 计算每部电影的评分计数、平均分和评分方差。
        2. 使用 QuantileDiscretizer 对评分计数分桶。
        3. 使用 MinMaxScaler 对平均评分进行归一化。
    """
    ratingSamples.printSchema()
    ratingSamples.show()
    # calculate average movie rating score and rating count
    movieFeatures = ratingSamples.groupBy('movieId').agg(F.count(F.lit(1)).alias('ratingCount'),
                                                         F.avg("rating").alias("avgRating"),
                                                         F.variance('rating').alias('ratingVar')) \
        .withColumn('avgRatingVec', udf(lambda x: Vectors.dense(x), VectorUDT())('avgRating'))
    movieFeatures.show(10)
    # bucketing
    ratingCountDiscretizer = QuantileDiscretizer(numBuckets=100, inputCol="ratingCount", outputCol="ratingCountBucket")
    # Normalization
    ratingScaler = MinMaxScaler(inputCol="avgRatingVec", outputCol="scaleAvgRating")
    pipelineStage = [ratingCountDiscretizer, ratingScaler]
    featurePipeline = Pipeline(stages=pipelineStage)
    movieProcessedFeatures = featurePipeline.fit(movieFeatures).transform(movieFeatures)
    movieProcessedFeatures.show(10)


if __name__ == '__main__':
    """
    主程序逻辑:
    1. 初始化 Spark 环境。
    2. 加载电影和评分数据。
    3. 分别调用 One-Hot 和 Multi-Hot 编码的示例函数。
    4. 处理评分数据并生成特征。
    """
    conf = SparkConf().setAppName('featureEngineering').setMaster('local')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    file_path = 'file:///Users/zhewang/Workspace/SparrowRecSys/src/main/resources'
    movieResourcesPath = file_path + "/webroot/sampledata/movies.csv"
    movieSamples = spark.read.format('csv').option('header', 'true').load(movieResourcesPath)
    print("Raw Movie Samples:")
    movieSamples.show(10)
    movieSamples.printSchema()
    print("OneHotEncoder Example:")
    oneHotEncoderExample(movieSamples)
    print("MultiHotEncoder Example:")
    multiHotEncoderExample(movieSamples)
    print("Numerical features Example:")
    ratingsResourcesPath = file_path + "/webroot/sampledata/ratings.csv"
    ratingSamples = spark.read.format('csv').option('header', 'true').load(ratingsResourcesPath)
    ratingFeatures(ratingSamples)

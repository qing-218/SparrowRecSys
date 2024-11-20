# 导入必要的库
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.mllib.feature import Word2Vec
from pyspark.ml.linalg import Vectors
import random
from collections import defaultdict
import numpy as np
from pyspark.sql import functions as F

# 自定义类，用于定义可复用的函数
"""
        根据时间戳排序电影列表，并返回排序后的电影序列
        :param movie_list: 电影ID列表
        :param timestamp_list: 时间戳列表
        :return: 按时间排序的电影ID列表
"""
class UdfFunction:
    @staticmethod
    def sortF(movie_list, timestamp_list):
        """
        sort by time and return the corresponding movie sequence
        eg:
            input: movie_list:[1,2,3]
                   timestamp_list:[1112486027,1212546032,1012486033]
            return [3,1,2]
        """
        # 将电影和时间戳配对
        pairs = []
        for m, t in zip(movie_list, timestamp_list):
            pairs.append((m, t))
        # 按时间排序
        pairs = sorted(pairs, key=lambda x: x[1])
        return [x[0] for x in pairs]


# 处理用户的电影观看序列数据
def processItemSequence(spark, rawSampleDataPath):
    """
    读取电影评分数据，过滤评分 >= 3.5 的记录，按用户分组并生成按时间排序的电影序列
    :param spark: SparkSession
    :param rawSampleDataPath: 输入的评分数据路径
    :return: 每个用户的电影ID序列的RDD
    """
    # rating data
    ratingSamples = spark.read.format("csv").option("header", "true").load(rawSampleDataPath)
    # ratingSamples.show(5)
    # ratingSamples.printSchema()
    sortUdf = udf(UdfFunction.sortF, ArrayType(StringType()))
    userSeq = ratingSamples \
        .where(F.col("rating") >= 3.5) \
        .groupBy("userId") \
        .agg(sortUdf(F.collect_list("movieId"), F.collect_list("timestamp")).alias('movieIds')) \
        .withColumn("movieIdStr", array_join(F.col("movieIds"), " "))
    # userSeq.select("userId", "movieIdStr").show(10, truncate = False)
    return userSeq.select('movieIdStr').rdd.map(lambda x: x[0].split(' '))


def embeddingLSH(spark, movieEmbMap):
    """
    使用BucketedRandomProjectionLSH对电影嵌入进行分桶，并查找最近邻
    :param spark: SparkSession
    :param movieEmbMap: 电影嵌入字典，键为电影ID，值为嵌入向量
    """
    movieEmbSeq = []
    # 将嵌入数据转换为DenseVector格式
    for key, embedding_list in movieEmbMap.items():
        embedding_list = [np.float64(embedding) for embedding in embedding_list]
        movieEmbSeq.append((key, Vectors.dense(embedding_list)))
    movieEmbDF = spark.createDataFrame(movieEmbSeq).toDF("movieId", "emb")
    # 定义LSH模型
    bucketProjectionLSH = BucketedRandomProjectionLSH(inputCol="emb", outputCol="bucketId", bucketLength=0.1,
                                                      numHashTables=3)
    bucketModel = bucketProjectionLSH.fit(movieEmbDF) #训练
    embBucketResult = bucketModel.transform(movieEmbDF) #应用
    print("movieId, emb, bucketId schema:")
    embBucketResult.printSchema()
    print("movieId, emb, bucketId data result:")
    embBucketResult.show(10, truncate=False)  # 显示前10条分桶结果
    print("Approximately searching for 5 nearest neighbors of the sample embedding:")
      # 查找最近邻
    sampleEmb = Vectors.dense(0.795, 0.583, 1.120, 0.850, 0.174, -0.839, -0.0633, 0.249, 0.673, -0.237)
    bucketModel.approxNearestNeighbors(movieEmbDF, sampleEmb, 5).show(truncate=False)

# 训练Item2Vec模型
def trainItem2vec(spark, samples, embLength, embOutputPath, saveToRedis, redisKeyPrefix):
    """
    使用Word2Vec训练电影嵌入模型，并保存结果
    :param spark: SparkSession
    :param samples: 输入的电影序列样本
    :param embLength: 嵌入向量长度
    :param embOutputPath: 嵌入输出路径
    :param saveToRedis: 是否保存到Redis
    :param redisKeyPrefix: Redis键前缀
    :return: Word2Vec模型
    """
    word2vec = Word2Vec().setVectorSize(embLength).setWindowSize(5).setNumIterations(10)
    model = word2vec.fit(samples)
    # 查看与电影ID"158"最相似的电影
    synonyms = model.findSynonyms("158", 20)
    for synonym, cosineSimilarity in synonyms:
        print(synonym, cosineSimilarity)
    # 保存嵌入向量
    embOutputDir = '/'.join(embOutputPath.split('/')[:-1])
    if not os.path.exists(embOutputDir):
        os.makedirs(embOutputDir)
    with open(embOutputPath, 'w') as f:
        for movie_id in model.getVectors():
            vectors = " ".join([str(emb) for emb in model.getVectors()[movie_id]])
            f.write(movie_id + ":" + vectors + "\n")
    embeddingLSH(spark, model.getVectors())
    return model

#生成一个序列中相邻项的配对列表。
#用于构建过渡矩阵时统计相邻物品的共现次数
def generate_pair(x):
    # eg:
    # watch sequence:['858', '50', '593', '457']
    # return:[['858', '50'],['50', '593'],['593', '457']]
    pairSeq = []
    previousItem = ''
    for item in x:
        if not previousItem:
            previousItem = item
        else:
            pairSeq.append((previousItem, item))
            previousItem = item
    return pairSeq

#根据物品序列样本生成转移概率矩阵和物品分布概率。
def generateTransitionMatrix(samples):
    """
    根据物品序列样本生成转移概率矩阵和物品分布概率。
    输入:
        samples (RDD): 每个元素是一个物品序列的 RDD。
    输出:
        tuple:
            - transitionMatrix (dict): 表示转移概率矩阵 {item1: {item2: prob}}。
            - itemDistribution (dict): 表示物品分布概率 {item: prob}。
    逻辑:
        - 生成相邻物品的配对。
        - 统计每个配对的出现次数，以及每个物品的总出现次数。
        - 计算转移概率 (配对次数 / 当前物品总出现次数)。
        - 计算物品分布概率 (物品总出现次数 / 配对总次数)。
    """
    pairSamples = samples.flatMap(lambda x: generate_pair(x))
    pairCountMap = pairSamples.countByValue()
    pairTotalCount = 0
    transitionCountMatrix = defaultdict(dict)
    itemCountMap = defaultdict(int)
    for key, cnt in pairCountMap.items():
        key1, key2 = key
        transitionCountMatrix[key1][key2] = cnt
        itemCountMap[key1] += cnt
        pairTotalCount += cnt
    transitionMatrix = defaultdict(dict)
    itemDistribution = defaultdict(dict)
    for key1, transitionMap in transitionCountMatrix.items():
        for key2, cnt in transitionMap.items():
            transitionMatrix[key1][key2] = transitionCountMatrix[key1][key2] / itemCountMap[key1]
    for itemid, cnt in itemCountMap.items():
        itemDistribution[itemid] = cnt / pairTotalCount
    return transitionMatrix, itemDistribution

#基于转移矩阵和物品分布概率，生成一个随机游走的物品序列。
def oneRandomWalk(transitionMatrix, itemDistribution, sampleLength):
    """
    基于转移矩阵和物品分布概率，生成一个随机游走序列。
    输入:
        transitionMatrix (dict): 转移概率矩阵。
        itemDistribution (dict): 物品分布概率。
        sampleLength (int): 随机游走的目标序列长度。
    输出:
        list: 随机游走生成的物品序列。
    逻辑:
        - 根据物品分布概率选择起始物品。
        - 根据转移概率依次选择下一个物品，直到达到目标长度或无有效转移。
    """
    sample = []
    # pick the first element
    randomDouble = random.random()
    firstItem = ""
    accumulateProb = 0.0
    for item, prob in itemDistribution.items():
        accumulateProb += prob
        if accumulateProb >= randomDouble:
            firstItem = item
            break
    sample.append(firstItem)
    curElement = firstItem
    i = 1
    while i < sampleLength:
        if (curElement not in itemDistribution) or (curElement not in transitionMatrix):
            break
        probDistribution = transitionMatrix[curElement]
        randomDouble = random.random()
        accumulateProb = 0.0
        for item, prob in probDistribution.items():
            accumulateProb += prob
            if accumulateProb >= randomDouble:
                curElement = item
                break
        sample.append(curElement)
        i += 1
    return sample

# 生成随机游走样本
def randomWalk(transitionMatrix, itemDistribution, sampleCount, sampleLength):
    """
    使用随机游走算法生成样本
    :param transitionMatrix: 状态转移矩阵
    :param itemDistribution: 初始分布
    :param sampleCount: 样本数量
    :param sampleLength: 样本长度
    :return: 随机游走样本列表
    """
    samples = []
    for i in range(sampleCount):
        samples.append(oneRandomWalk(transitionMatrix, itemDistribution, sampleLength))
    return samples

# 图嵌入训练
def graphEmb(samples, spark, embLength, embOutputFilename, saveToRedis, redisKeyPrefix):
    """
    基于用户序列构建状态转移矩阵并训练图嵌入
    :param samples: 用户序列
    :param spark: SparkSession
    :param embLength: 嵌入向量长度
    :param embOutputFilename: 嵌入输出文件名
    :param saveToRedis: 是否保存到Redis
    :param redisKeyPrefix: Redis键前缀
    """
    transitionMatrix, itemDistribution = generateTransitionMatrix(samples)# 生成状态转移矩阵
    sampleCount = 20000
    sampleLength = 10 # 生成随机游走样本
    newSamples = randomWalk(transitionMatrix, itemDistribution, sampleCount, sampleLength)
    rddSamples = spark.sparkContext.parallelize(newSamples)# 转换为RDD
    trainItem2vec(spark, rddSamples, embLength, embOutputFilename, saveToRedis, redisKeyPrefix) # 训练Item2Vec

#生成用户的嵌入表示（向量）
def generateUserEmb(spark, rawSampleDataPath, model, embLength, embOutputPath, saveToRedis, redisKeyPrefix):
    """
    生成用户嵌入表示。
    输入:
        spark (SparkSession): Spark 会话。
        rawSampleDataPath (str): 用户-物品评分数据路径。
        model: 物品嵌入模型 (Word2Vec 模型)。
        embLength (int): 嵌入向量的长度。
        embOutputPath (str): 用户嵌入的保存路径。
        saveToRedis (bool): 是否保存到 Redis。
        redisKeyPrefix (str): Redis 键的前缀。
    输出:
        None: 嵌入结果保存到文件中。
    逻辑:
        - 加载评分数据并提取用户和物品关系。
        - 从模型中获取物品的嵌入向量，并与评分数据关联。
        - 对每个用户计算其关联物品的嵌入向量之和。
        - 保存用户嵌入到指定路径。
    """
    ratingSamples = spark.read.format("csv").option("header", "true").load(rawSampleDataPath)
    Vectors_list = []
    for key, value in model.getVectors().items():
        Vectors_list.append((key, list(value)))
    fields = [
        StructField('movieId', StringType(), False),
        StructField('emb', ArrayType(FloatType()), False)
    ]
    schema = StructType(fields)
    Vectors_df = spark.createDataFrame(Vectors_list, schema=schema)
    ratingSamples = ratingSamples.join(Vectors_df, on='movieId', how='inner')
    result = ratingSamples.select('userId', 'emb').rdd.map(lambda x: (x[0], x[1])) \
        .reduceByKey(lambda a, b: [a[i] + b[i] for i in range(len(a))]).collect()
    with open(embOutputPath, 'w') as f:
        for row in result:
            vectors = " ".join([str(emb) for emb in row[1]])
            f.write(row[0] + ":" + vectors + "\n")


if __name__ == '__main__':
    conf = SparkConf().setAppName('ctrModel').setMaster('local') # 配置Spark
    spark = SparkSession.builder.config(conf=conf).getOrCreate() # 创建SparkSession
    # Change to your own filepath
    file_path = 'file:///home/hadoop/SparrowRecSys/src/main/resources'
    rawSampleDataPath = file_path + "/webroot/sampledata/ratings.csv"
    embLength = 10 # 嵌入向量长度
    # 处理数据并训练模型
    samples = processItemSequence(spark, rawSampleDataPath)
    model = trainItem2vec(spark, samples, embLength,
                          embOutputPath=file_path[7:] + "/webroot/modeldata2/item2vecEmb.csv", saveToRedis=False,
                          redisKeyPrefix="i2vEmb")
    graphEmb(samples, spark, embLength, embOutputFilename=file_path[7:] + "/webroot/modeldata2/itemGraphEmb.csv",
             saveToRedis=True, redisKeyPrefix="graphEmb")
    generateUserEmb(spark, rawSampleDataPath, model, embLength,
                    embOutputPath=file_path[7:] + "/webroot/modeldata2/userEmb.csv", saveToRedis=False,
                    redisKeyPrefix="uEmb")

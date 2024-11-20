from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql as sql
from pyspark.sql.functions import *
from pyspark.sql.types import *
from collections import defaultdict
from pyspark.sql import functions as F

NUMBER_PRECISION = 2


def addSampleLabel(ratingSamples):
    """
    为评分样本数据添加标签 (label)。
    输入:
        ratingSamples (DataFrame): 包含用户评分数据的 DataFrame，必须包括 'rating' 列。
    输出:
        DataFrame: 在原始 DataFrame 基础上新增一列 'label'，值为 1 或 0。
    逻辑:
        - 展示前 5 行数据及其架构，便于调试和验证。
        - 按照评分值分组，计算每个评分的数量及占比。
        - 添加新列 'label'：
            - 如果评分 >= 3.5，则设置为 1 (正样本)。
            - 否则设置为 0 (负样本)。
    """
    ratingSamples.show(5, truncate=False)
    ratingSamples.printSchema()
    sampleCount = ratingSamples.count() # 样本总数
    ratingSamples.groupBy('rating').count().orderBy('rating').withColumn('percentage',
                                                                         F.col('count') / sampleCount).show() # 统计评分分布
    ratingSamples = ratingSamples.withColumn('label', when(F.col('rating') >= 3.5, 1).otherwise(0)) # 添加标签列
    return ratingSamples


def extractReleaseYearUdf(title):
    # add realease year
    """
    从电影标题中提取上映年份。
    输入:
        title (str): 电影标题，例如 "The Shawshank Redemption (1994)"。
    输出:
        int: 上映年份。如果无法解析年份，默认返回 1990。
    逻辑:
        - 检查标题的长度是否足够长，否则返回默认值 1990。
        - 提取标题中最后 4 位数字作为年份。
    """
    if not title or len(title.strip()) < 6:
        return 1990
    else:
        yearStr = title.strip()[-5:-1]
    return int(yearStr)


def addMovieFeatures(movieSamples, ratingSamplesWithLabel):
    """
    为样本数据添加电影特征，包括上映年份、类型及评分相关统计信息。
    输入:
        movieSamples (DataFrame): 包含电影元数据信息的 DataFrame。
        ratingSamplesWithLabel (DataFrame): 已标注正负样本标签的评分数据。
    输出:
        DataFrame: 包含额外电影特征的新数据集。
    逻辑:
        - 将评分数据与电影数据通过 'movieId' 连接。
        - 提取上映年份并清洗标题。
        - 分割电影类型 (genres)，提取最多 3 个类型。
        - 计算电影的评分统计特征 (总评分次数、平均评分、评分标准差)。
        - 合并以上特征到一个 DataFrame。
    """
    # add movie basic features
    samplesWithMovies1 = ratingSamplesWithLabel.join(movieSamples, on=['movieId'], how='left')
    # add releaseYear,title
    samplesWithMovies2 = samplesWithMovies1.withColumn('releaseYear',
                                                       udf(extractReleaseYearUdf, IntegerType())('title')) \
        .withColumn('title', udf(lambda x: x.strip()[:-6].strip(), StringType())('title')) \
        .drop('title')
    # split genres
    samplesWithMovies3 = samplesWithMovies2.withColumn('movieGenre1', split(F.col('genres'), "\\|")[0]) \
        .withColumn('movieGenre2', split(F.col('genres'), "\\|")[1]) \
        .withColumn('movieGenre3', split(F.col('genres'), "\\|")[2])
    # add rating features
    movieRatingFeatures = samplesWithMovies3.groupBy('movieId').agg(F.count(F.lit(1)).alias('movieRatingCount'),
                                                                    format_number(F.avg(F.col('rating')),
                                                                                  NUMBER_PRECISION).alias(
                                                                        'movieAvgRating'),
                                                                    F.stddev(F.col('rating')).alias(
                                                                        'movieRatingStddev')).fillna(0) \
        .withColumn('movieRatingStddev', format_number(F.col('movieRatingStddev'), NUMBER_PRECISION))
    # join movie rating features
    samplesWithMovies4 = samplesWithMovies3.join(movieRatingFeatures, on=['movieId'], how='left')
    samplesWithMovies4.printSchema()
    samplesWithMovies4.show(5, truncate=False)
    return samplesWithMovies4


def extractGenres(genres_list):
    """
    提取类型列表中的每种类型的出现频率，并按频率降序排列。
    输入:
        genres_list (list): 包含类型信息的字符串列表，例如 ["Action|Adventure", "Action|Sci-Fi"]。
    输出:
        list: 按频率排序的类型列表，例如 ['Action', 'Adventure', 'Sci-Fi']。
    逻辑:
        - 遍历每个字符串，统计其中各类型的出现次数。
        - 按出现次数降序排序。
        - 返回类型列表。
    """
    '''
    pass in a list which format like ["Action|Adventure|Sci-Fi|Thriller", "Crime|Horror|Thriller"]
    count by each genre，return genre_list in reverse order
    eg:
    (('Thriller',2),('Action',1),('Sci-Fi',1),('Horror', 1), ('Adventure',1),('Crime',1))
    return:['Thriller','Action','Sci-Fi','Horror','Adventure','Crime']
    '''
    genres_dict = defaultdict(int)
    for genres in genres_list:
        for genre in genres.split('|'):
            genres_dict[genre] += 1
    sortedGenres = sorted(genres_dict.items(), key=lambda x: x[1], reverse=True)
    return [x[0] for x in sortedGenres]


def addUserFeatures(samplesWithMovieFeatures):
    """
    为样本数据添加用户特征，包括用户历史行为及评分统计信息。
    输入:
        samplesWithMovieFeatures (DataFrame): 包含电影特征的样本数据。
    输出:
        DataFrame: 包含额外用户特征的新数据集。
    逻辑:
        - 提取用户的正样本观看历史，最多包含 5 条记录。
        - 计算用户的评分次数、评分平均值、评分标准差等特征。
        - 提取用户偏好的类型列表。
    """
    extractGenresUdf = udf(extractGenres, ArrayType(StringType()))
    samplesWithUserFeatures = samplesWithMovieFeatures \
        .withColumn('userPositiveHistory',
                    F.collect_list(when(F.col('label') == 1, F.col('movieId')).otherwise(F.lit(None))).over(
                        sql.Window.partitionBy("userId").orderBy(F.col("timestamp")).rowsBetween(-100, -1))) \
        .withColumn("userPositiveHistory", reverse(F.col("userPositiveHistory"))) \
        .withColumn('userRatedMovie1', F.col('userPositiveHistory')[0]) \
        .withColumn('userRatedMovie2', F.col('userPositiveHistory')[1]) \
        .withColumn('userRatedMovie3', F.col('userPositiveHistory')[2]) \
        .withColumn('userRatedMovie4', F.col('userPositiveHistory')[3]) \
        .withColumn('userRatedMovie5', F.col('userPositiveHistory')[4]) \
        .withColumn('userRatingCount',
                    F.count(F.lit(1)).over(sql.Window.partitionBy('userId').orderBy('timestamp').rowsBetween(-100, -1))) \
        .withColumn('userAvgReleaseYear', F.avg(F.col('releaseYear')).over(
        sql.Window.partitionBy('userId').orderBy('timestamp').rowsBetween(-100, -1)).cast(IntegerType())) \
        .withColumn('userReleaseYearStddev', F.stddev(F.col("releaseYear")).over(
        sql.Window.partitionBy('userId').orderBy('timestamp').rowsBetween(-100, -1))) \
        .withColumn("userAvgRating", format_number(
        F.avg(F.col("rating")).over(sql.Window.partitionBy('userId').orderBy('timestamp').rowsBetween(-100, -1)),
        NUMBER_PRECISION)) \
        .withColumn("userRatingStddev", F.stddev(F.col("rating")).over(
        sql.Window.partitionBy('userId').orderBy('timestamp').rowsBetween(-100, -1))) \
        .withColumn("userGenres", extractGenresUdf(
        F.collect_list(when(F.col('label') == 1, F.col('genres')).otherwise(F.lit(None))).over(
            sql.Window.partitionBy('userId').orderBy('timestamp').rowsBetween(-100, -1)))) \
        .withColumn("userRatingStddev", format_number(F.col("userRatingStddev"), NUMBER_PRECISION)) \
        .withColumn("userReleaseYearStddev", format_number(F.col("userReleaseYearStddev"), NUMBER_PRECISION)) \
        .withColumn("userGenre1", F.col("userGenres")[0]) \
        .withColumn("userGenre2", F.col("userGenres")[1]) \
        .withColumn("userGenre3", F.col("userGenres")[2]) \
        .withColumn("userGenre4", F.col("userGenres")[3]) \
        .withColumn("userGenre5", F.col("userGenres")[4]) \
        .drop("genres", "userGenres", "userPositiveHistory") \
        .filter(F.col("userRatingCount") > 1)
    samplesWithUserFeatures.printSchema()
    samplesWithUserFeatures.show(10)
    samplesWithUserFeatures.filter(samplesWithMovieFeatures['userId'] == 1).orderBy(F.col('timestamp').asc()).show(
        truncate=False)
    return samplesWithUserFeatures


def splitAndSaveTrainingTestSamples(samplesWithUserFeatures, file_path):
    """
    随机划分训练集和测试集，并保存为 CSV 文件。
    输入:
        samplesWithUserFeatures (DataFrame): 包含用户和电影特征的完整样本数据。
        file_path (str): 用于保存训练集和测试集的文件路径。
    输出:
        无直接返回值。结果保存为 CSV 文件，分为训练集和测试集。
    逻辑:
        1. 从原始数据中随机抽取 10% 的样本，用于快速测试。
        2. 将抽取的数据随机划分为 80% 的训练集和 20% 的测试集。
        3. 保存训练集和测试集到指定路径，每个数据集仅分为一个文件。
    """
    smallSamples = samplesWithUserFeatures.sample(0.1)
    training, test = smallSamples.randomSplit((0.8, 0.2))
    trainingSavePath = file_path + '/trainingSamples'
    testSavePath = file_path + '/testSamples'
    training.repartition(1).write.option("header", "true").mode('overwrite') \
        .csv(trainingSavePath)
    test.repartition(1).write.option("header", "true").mode('overwrite') \
        .csv(testSavePath)


def splitAndSaveTrainingTestSamplesByTimeStamp(samplesWithUserFeatures, file_path):
    """
    基于时间戳划分训练集和测试集，并保存为 CSV 文件。
    输入:
        samplesWithUserFeatures (DataFrame): 包含用户和电影特征的完整样本数据。
        file_path (str): 用于保存训练集和测试集的文件路径。
    输出:
        无直接返回值。结果保存为 CSV 文件，分为训练集和测试集。
    逻辑:
        1. 从原始数据中随机抽取 10% 的样本，用于快速测试。
        2. 将时间戳 (timestamp) 转换为 long 类型，便于数值比较。
        3. 根据时间戳的 80% 分位数将数据划分为训练集和测试集：
            - 时间戳小于或等于分位数的样本归为训练集。
            - 时间戳大于分位数的样本归为测试集。
        4. 保存训练集和测试集到指定路径，每个数据集仅分为一个文件。
    """
    smallSamples = samplesWithUserFeatures.sample(0.1).withColumn("timestampLong", F.col("timestamp").cast(LongType()))
    quantile = smallSamples.stat.approxQuantile("timestampLong", [0.8], 0.05)
    splitTimestamp = quantile[0]
    training = smallSamples.where(F.col("timestampLong") <= splitTimestamp).drop("timestampLong")
    test = smallSamples.where(F.col("timestampLong") > splitTimestamp).drop("timestampLong")
    trainingSavePath = file_path + '/trainingSamples'
    testSavePath = file_path + '/testSamples'
    training.repartition(1).write.option("header", "true").mode('overwrite') \
        .csv(trainingSavePath)
    test.repartition(1).write.option("header", "true").mode('overwrite') \
        .csv(testSavePath)


if __name__ == '__main__':
    """
    主程序逻辑：
    1. 初始化 Spark 环境。
    2. 加载电影和评分数据集。
    3. 执行特征工程步骤：添加标签、电影特征、用户特征。
    4. 随机划分训练集和测试集，或基于时间戳划分，并保存结果。
    """
    conf = SparkConf().setAppName('featureEngineering').setMaster('local')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    file_path = 'file:///home/hadoop/SparrowRecSys/src/main/resources'
    movieResourcesPath = file_path + "/webroot/sampledata/movies.csv"
    ratingsResourcesPath = file_path + "/webroot/sampledata/ratings.csv"
    movieSamples = spark.read.format('csv').option('header', 'true').load(movieResourcesPath)
    ratingSamples = spark.read.format('csv').option('header', 'true').load(ratingsResourcesPath)
    ratingSamplesWithLabel = addSampleLabel(ratingSamples)
    ratingSamplesWithLabel.show(10, truncate=False)
    samplesWithMovieFeatures = addMovieFeatures(movieSamples, ratingSamplesWithLabel)
    samplesWithUserFeatures = addUserFeatures(samplesWithMovieFeatures)
    # save samples as csv format
    splitAndSaveTrainingTestSamples(samplesWithUserFeatures, file_path + "/webroot/sampledata")
    # splitAndSaveTrainingTestSamplesByTimeStamp(samplesWithUserFeatures, file_path + "/webroot/sampledata")

package com.sparrowrecsys.offline.spark.featureeng

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{format_number, _}
import org.apache.spark.sql.types.{DecimalType, FloatType, IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.SetParams

import scala.collection.immutable.ListMap
import scala.collection.{JavaConversions, mutable}

object FeatureEngForRecModel {

  val NUMBER_PRECISION = 2 // 数值保留小数点后两位精度
  val redisEndpoint = "localhost" // Redis 服务端地址
  val redisPort = 6379 // Redis 服务端端口号

  /**
   * 为评分样本添加二分类标签
   * 如果评分大于等于3.5，标签为1；否则为0
   * @param ratingSamples 包含评分的样本数据
   * @return 添加了标签列的DataFrame
   */
  def addSampleLabel(ratingSamples: DataFrame): DataFrame = {
    ratingSamples.show(10, truncate = false) // 展示评分样本的前10条数据
    ratingSamples.printSchema() // 打印评分样本的模式信息
    val sampleCount = ratingSamples.count() // 样本总数
    // 按评分统计每个评分的数量及其占比
    ratingSamples.groupBy(col("rating")).count().orderBy(col("rating"))
      .withColumn("percentage", col("count") / sampleCount).show(100, truncate = false)

    // 添加标签列，评分大于等于3.5为正样本（1），否则为负样本（0）
    ratingSamples.withColumn("label", when(col("rating") >= 3.5, 1).otherwise(0))
  }

  /**
   * 添加电影特征
   * 包括基本信息、年份、类型以及统计特征
   * @param movieSamples 电影信息样本数据
   * @param ratingSamples 用户评分样本数据
   * @return 添加了电影特征的DataFrame
   */
  def addMovieFeatures(movieSamples: DataFrame, ratingSamples: DataFrame): DataFrame = {

    // 添加电影基本信息
    val samplesWithMovies1 = ratingSamples.join(movieSamples, Seq("movieId"), "left")

    // 提取电影的发行年份
    val extractReleaseYearUdf = udf({ (title: String) => {
      if (null == title || title.trim.length < 6) {
        1990 // 默认值
      } else {
        val yearString = title.trim.substring(title.length - 5, title.length - 1)
        yearString.toInt
      }
    }})

    // 提取电影的标题
    val extractTitleUdf = udf({ (title: String) => {
      title.trim.substring(0, title.trim.length - 6).trim
    }})

    val samplesWithMovies2 = samplesWithMovies1.withColumn("releaseYear", extractReleaseYearUdf(col("title")))
      .withColumn("title", extractTitleUdf(col("title")))
      .drop("title") // 当前不需要标题列

    // 分割电影类型（genres）为多个字段
    val samplesWithMovies3 = samplesWithMovies2.withColumn("movieGenre1", split(col("genres"), "\\|").getItem(0))
      .withColumn("movieGenre2", split(col("genres"), "\\|").getItem(1))
      .withColumn("movieGenre3", split(col("genres"), "\\|").getItem(2))

    // 统计电影的评分特征
    val movieRatingFeatures = samplesWithMovies3.groupBy(col("movieId"))
      .agg(count(lit(1)).as("movieRatingCount"), // 评分数量
        format_number(avg(col("rating")), NUMBER_PRECISION).as("movieAvgRating"), // 平均评分
        stddev(col("rating")).as("movieRatingStddev")) // 评分标准差
      .na.fill(0).withColumn("movieRatingStddev", format_number(col("movieRatingStddev"), NUMBER_PRECISION))

    // 合并评分统计特征到样本数据中
    val samplesWithMovies4 = samplesWithMovies3.join(movieRatingFeatures, Seq("movieId"), "left")
    samplesWithMovies4.printSchema() // 打印新表的模式信息
    samplesWithMovies4.show(10, truncate = false) // 展示前10条数据

    samplesWithMovies4
  }

  /**
   * 提取用户特征
   * 包括用户的评分历史、偏好、评分分布等信息
   * @param ratingSamples 用户评分样本数据
   * @return 添加了用户特征的DataFrame
   */
  def addUserFeatures(ratingSamples: DataFrame): DataFrame = {
    val samplesWithUserFeatures = ratingSamples
      // 收集用户最近的评分历史
      .withColumn("userPositiveHistory", collect_list(when(col("label") === 1, col("movieId")).otherwise(lit(null)))
        .over(Window.partitionBy("userId").orderBy(col("timestamp")).rowsBetween(-100, -1)))
      .withColumn("userPositiveHistory", reverse(col("userPositiveHistory")))
      // 提取用户最近评分的电影ID
      .withColumn("userRatedMovie1", col("userPositiveHistory").getItem(0))
      .withColumn("userRatedMovie2", col("userPositiveHistory").getItem(1))
      .withColumn("userRatedMovie3", col("userPositiveHistory").getItem(2))
      .withColumn("userRatedMovie4", col("userPositiveHistory").getItem(3))
      .withColumn("userRatedMovie5", col("userPositiveHistory").getItem(4))
      // 用户评分数量
      .withColumn("userRatingCount", count(lit(1))
        .over(Window.partitionBy("userId").orderBy(col("timestamp")).rowsBetween(-100, -1)))
      // 用户评分电影的平均发行年份
      .withColumn("userAvgReleaseYear", avg(col("releaseYear"))
        .over(Window.partitionBy("userId").orderBy(col("timestamp")).rowsBetween(-100, -1)).cast(IntegerType))
      // 用户评分电影发行年份的标准差
      .withColumn("userReleaseYearStddev", stddev(col("releaseYear"))
        .over(Window.partitionBy("userId").orderBy(col("timestamp")).rowsBetween(-100, -1)))
      // 用户评分的平均分
      .withColumn("userAvgRating", format_number(avg(col("rating"))
        .over(Window.partitionBy("userId").orderBy(col("timestamp")).rowsBetween(-100, -1)), NUMBER_PRECISION))
      // 用户评分的标准差
      .withColumn("userRatingStddev", stddev(col("rating"))
        .over(Window.partitionBy("userId").orderBy(col("timestamp")).rowsBetween(-100, -1)))
      // 用户偏好的电影类型
      .withColumn("userGenres", extractGenres(collect_list(when(col("label") === 1, col("genres")).otherwise(lit(null)))
        .over(Window.partitionBy("userId").orderBy(col("timestamp")).rowsBetween(-100, -1))))
      .na.fill(0)
      .withColumn("userRatingStddev", format_number(col("userRatingStddev"), NUMBER_PRECISION))
      .withColumn("userReleaseYearStddev", format_number(col("userReleaseYearStddev"), NUMBER_PRECISION))
      // 提取用户偏好的前5个电影类型
      .withColumn("userGenre1", col("userGenres").getItem(0))
      .withColumn("userGenre2", col("userGenres").getItem(1))
      .withColumn("userGenre3", col("userGenres").getItem(2))
      .withColumn("userGenre4", col("userGenres").getItem(3))
      .withColumn("userGenre5", col("userGenres").getItem(4))
      .drop("genres", "userGenres", "userPositiveHistory") // 删除中间过程的列
      .filter(col("userRatingCount") > 1) // 过滤评分数量大于1的用户

    samplesWithUserFeatures.printSchema() // 打印新表的模式信息
    samplesWithUserFeatures.show(100, truncate = false) // 展示前100条数据

    samplesWithUserFeatures
  }

def extractAndSaveMovieFeaturesToRedis(samples:DataFrame): DataFrame = {
    // 为每部电影提取最新的评分记录并保留其相关特征
    val movieLatestSamples = samples.withColumn("movieRowNum", row_number()
      .over(Window.partitionBy("movieId") // 按照电影 ID 分组
        .orderBy(col("timestamp").desc))) // 按时间戳降序排列
      .filter(col("movieRowNum") === 1) // 只保留最新的评分记录
      .select("movieId","releaseYear", "movieGenre1","movieGenre2","movieGenre3","movieRatingCount",
        "movieAvgRating", "movieRatingStddev")
      .na.fill("") // 用空字符串填充缺失值

    movieLatestSamples.printSchema() // 打印数据表的模式信息
    movieLatestSamples.show(100, truncate = false) // 显示前 100 条数据，完整展示字段

    val movieFeaturePrefix = "mf:" // Redis 中电影特征的键前缀

    // 创建 Redis 客户端
    val redisClient = new Jedis(redisEndpoint, redisPort)
    val params = SetParams.setParams()
    // 设置键的生存时间为 30 天
    params.ex(60 * 60 * 24 * 30)

    // 收集所有电影记录到本地数组中
    val sampleArray = movieLatestSamples.collect()
    println("total movie size:" + sampleArray.length) // 打印总电影数量
    var insertedMovieNumber = 0
    val movieCount = sampleArray.length

    // 遍历每条电影记录，将其存入 Redis
    for (sample <- sampleArray){
      val movieKey = movieFeaturePrefix + sample.getAs[String]("movieId") // 构造 Redis 键
      val valueMap = mutable.Map[String, String]()
      valueMap("movieGenre1") = sample.getAs[String]("movieGenre1") // 存储第一类型
      valueMap("movieGenre2") = sample.getAs[String]("movieGenre2") // 存储第二类型
      valueMap("movieGenre3") = sample.getAs[String]("movieGenre3") // 存储第三类型
      valueMap("movieRatingCount") = sample.getAs[Long]("movieRatingCount").toString // 存储评分数量
      valueMap("releaseYear") = sample.getAs[Int]("releaseYear").toString // 存储上映年份
      valueMap("movieAvgRating") = sample.getAs[String]("movieAvgRating") // 存储平均评分
      valueMap("movieRatingStddev") = sample.getAs[String]("movieRatingStddev") // 存储评分标准差

      redisClient.hset(movieKey, JavaConversions.mapAsJavaMap(valueMap)) // 将数据存入 Redis 哈希
      insertedMovieNumber += 1
      if (insertedMovieNumber % 100 == 0) { // 每处理 100 条记录打印一次进度
        println(insertedMovieNumber + "/" + movieCount + "...")
      }
    }

    redisClient.close() // 关闭 Redis 连接
    movieLatestSamples // 返回最新电影特征的 DataFrame
}


def splitAndSaveTrainingTestSamples(samples:DataFrame, savePath:String)={
    // 生成一个较小的样本集用于演示，这里随机抽取原数据的 10%
    val smallSamples = samples.sample(0.1)

    // 将样本集按 8:2 的比例随机分成训练集和测试集
    val Array(training, test) = smallSamples.randomSplit(Array(0.8, 0.2))

    // 获取保存路径的资源文件目录
    val sampleResourcesPath = this.getClass.getResource(savePath)

    // 将训练集保存为 CSV 文件，覆盖原有文件
    training.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(sampleResourcesPath + "/trainingSamples")
    
    // 将测试集保存为 CSV 文件，覆盖原有文件
    test.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(sampleResourcesPath + "/testSamples")
}

def splitAndSaveTrainingTestSamplesByTimeStamp(samples:DataFrame, savePath:String)={
    // 生成一个较小的样本集用于演示，这里随机抽取原数据的 10%
    val smallSamples = samples.sample(0.1).withColumn("timestampLong", col("timestamp").cast(LongType)) 
    // 将时间戳转换为 Long 型，便于计算分割点

    // 使用近似分位数算法，计算样本集中时间戳的 80% 分位值作为分割点
    val quantile = smallSamples.stat.approxQuantile("timestampLong", Array(0.8), 0.05)
    val splitTimestamp = quantile.apply(0) // 提取分位值

    // 将样本集按时间分割，生成训练集和测试集
    val training = smallSamples.where(col("timestampLong") <= splitTimestamp).drop("timestampLong")
    val test = smallSamples.where(col("timestampLong") > splitTimestamp).drop("timestampLong")

    // 获取保存路径的资源文件目录
    val sampleResourcesPath = this.getClass.getResource(savePath)

    // 保存训练集到 CSV 文件
    training.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(sampleResourcesPath + "/trainingSamples")
    
    // 保存测试集到 CSV 文件
    test.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(sampleResourcesPath + "/testSamples")
}

def extractAndSaveUserFeaturesToRedis(samples:DataFrame): DataFrame = {
    // 为每个用户提取最新的评分记录并保留其相关特征
    val userLatestSamples = samples.withColumn("userRowNum", row_number()
      .over(Window.partitionBy("userId") // 按用户 ID 分组
        .orderBy(col("timestamp").desc))) // 按时间戳降序排列
      .filter(col("userRowNum") === 1) // 只保留最新的评分记录
      .select("userId","userRatedMovie1", "userRatedMovie2","userRatedMovie3","userRatedMovie4","userRatedMovie5",
        "userRatingCount", "userAvgReleaseYear", "userReleaseYearStddev", "userAvgRating", "userRatingStddev",
        "userGenre1", "userGenre2","userGenre3","userGenre4","userGenre5")
      .na.fill("") // 用空字符串填充缺失值

    userLatestSamples.printSchema() // 打印数据表的模式信息
    userLatestSamples.show(100, truncate = false) // 显示前 100 条数据，完整展示字段

    val userFeaturePrefix = "uf:" // Redis 中用户特征的键前缀

    // 创建 Redis 客户端
    val redisClient = new Jedis(redisEndpoint, redisPort)
    val params = SetParams.setParams()
    // 设置键的生存时间为 30 天
    params.ex(60 * 60 * 24 * 30)

    // 收集所有用户记录到本地数组中
    val sampleArray = userLatestSamples.collect()
    println("total user size:" + sampleArray.length) // 打印总用户数量
    var insertedUserNumber = 0
    val userCount = sampleArray.length

    // 遍历每条用户记录，将其存入 Redis
    for (sample <- sampleArray){
      val userKey = userFeaturePrefix + sample.getAs[String]("userId") // 构造 Redis 键
      val valueMap = mutable.Map[String, String]()
      valueMap("userRatedMovie1") = sample.getAs[String]("userRatedMovie1") // 存储用户评分的第一部电影
      valueMap("userRatedMovie2") = sample.getAs[String]("userRatedMovie2") // 存储用户评分的第二部电影
      valueMap("userRatedMovie3") = sample.getAs[String]("userRatedMovie3") // 存储用户评分的第三部电影
      valueMap("userRatedMovie4") = sample.getAs[String]("userRatedMovie4") // 存储用户评分的第四部电影
      valueMap("userRatedMovie5") = sample.getAs[String]("userRatedMovie5") // 存储用户评分的第五部电影
      valueMap("userGenre1") = sample.getAs[String]("userGenre1") // 存储用户喜欢的第一类型
      valueMap("userGenre2") = sample.getAs[String]("userGenre2") // 存储用户喜欢的第二类型
      valueMap("userGenre3") = sample.getAs[String]("userGenre3") // 存储用户喜欢的第三类型
      valueMap("userGenre4") = sample.getAs[String]("userGenre4") // 存储用户喜欢的第四类型
      valueMap("userGenre5") = sample.getAs[String]("userGenre5") // 存储用户喜欢的第五类型
      valueMap("userRatingCount") = sample.getAs[Long]("userRatingCount").toString // 存储评分数量
      valueMap("userAvgReleaseYear") = sample.getAs[Int]("userAvgReleaseYear").toString // 存储平均上映年份
      valueMap("userReleaseYearStddev") = sample.getAs[String]("userReleaseYearStddev") // 存储上映年份标准差
      valueMap("userAvgRating") = sample.getAs[String]("userAvgRating") // 存储平均评分
      valueMap("userRatingStddev") = sample.getAs[String]("userRatingStddev") // 存储评分标准差

      redisClient.hset(userKey, JavaConversions.mapAsJavaMap(valueMap)) // 将数据存入 Redis 哈希
      insertedUserNumber += 1
      if (insertedUserNumber % 100 == 0) { // 每处理 100 条记录打印一次进度
        println(insertedUserNumber + "/" + userCount + "...")
      }
    }

    redisClient.close() // 关闭 Redis 连接
    userLatestSamples // 返回最新用户特征的 DataFrame
}


  def main(args: Array[String]): Unit = {
    // 设置日志级别为 ERROR，避免输出过多的无关日志信息
    Logger.getLogger("org").setLevel(Level.ERROR)

    // 创建 Spark 配置对象，并设置运行模式为本地，应用名称为 "featureEngineering"
    val conf = new SparkConf()
      .setMaster("local") // 使用本地模式运行
      .setAppName("featureEngineering") // 设置应用程序名称
      .set("spark.submit.deployMode", "client") // 指定提交模式为客户端模式

    // 初始化 SparkSession，用于后续的 Spark 操作
    val spark = SparkSession.builder.config(conf).getOrCreate()

    // 加载电影数据集的路径
    val movieResourcesPath = this.getClass.getResource("/webroot/sampledata/movies.csv")
    // 读取电影数据集（CSV 格式，包含表头）
    val movieSamples = spark.read.format("csv").option("header", "true").load(movieResourcesPath.getPath)

    // 加载评分数据集的路径
    val ratingsResourcesPath = this.getClass.getResource("/webroot/sampledata/ratings.csv")
    // 读取评分数据集（CSV 格式，包含表头）
    val ratingSamples = spark.read.format("csv").option("header", "true").load(ratingsResourcesPath.getPath)

    // 调用 `addSampleLabel` 方法，为评分样本添加标签（例如正负样本标注）
    val ratingSamplesWithLabel = addSampleLabel(ratingSamples)
    // 显示带标签的评分样本的前 10 条数据，完全展开列内容
    ratingSamplesWithLabel.show(10, truncate = false)

    // 调用 `addMovieFeatures` 方法，将电影特征添加到评分样本中
    val samplesWithMovieFeatures = addMovieFeatures(movieSamples, ratingSamplesWithLabel)
    // 调用 `addUserFeatures` 方法，将用户特征添加到样本中
    val samplesWithUserFeatures = addUserFeatures(samplesWithMovieFeatures)

    // 将包含用户和电影特征的样本数据分割为训练集和测试集，并保存为 CSV 文件
    splitAndSaveTrainingTestSamples(samplesWithUserFeatures, "/webroot/sampledata")

    // 将用户和电影的特征提取后保存到 Redis 中，用于在线推理
    // 注意：以下两个方法默认被注释掉，需要时再取消注释使用
    // extractAndSaveUserFeaturesToRedis(samplesWithUserFeatures) // 提取并保存用户特征
    // extractAndSaveMovieFeaturesToRedis(samplesWithUserFeatures) // 提取并保存电影特征

    // 关闭 SparkSession，释放资源
    spark.close()
}

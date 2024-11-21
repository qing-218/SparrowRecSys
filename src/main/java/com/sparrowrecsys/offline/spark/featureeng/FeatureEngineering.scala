/*功能描述
One-hot 编码：将 movieId 转换为 one-hot 向量，适用于分类特征处理。
Multi-hot 编码：将电影的多个类型（genre）用 multi-hot 编码表示，生成稀疏向量。
评分特征处理：
计算电影的评分计数、平均评分和评分方差；
使用分桶和归一化技术对特征进行处理。
主函数：
加载电影和评分数据集；
展示编码和特征处理的具体实现。*/

package com.sparrowrecsys.offline.spark.featureeng

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object FeatureEngineering {

  /**
   * One-hot 编码示例函数
   * @param samples 电影样本数据集（DataFrame 格式）
   */
  def oneHotEncoderExample(samples: DataFrame): Unit = {
    // 将 movieId 转换为整型，便于后续处理
    val samplesWithIdNumber = samples.withColumn("movieIdNumber", col("movieId").cast(sql.types.IntegerType))

    // 创建 OneHotEncoderEstimator 实例，指定输入和输出列
    val oneHotEncoder = new OneHotEncoderEstimator()
      .setInputCols(Array("movieIdNumber")) // 输入列
      .setOutputCols(Array("movieIdVector")) // 输出列
      .setDropLast(false) // 不删除最后一个类别

    // 执行 one-hot 编码并展示结果
    val oneHotEncoderSamples = oneHotEncoder.fit(samplesWithIdNumber).transform(samplesWithIdNumber)
    oneHotEncoderSamples.printSchema()
    oneHotEncoderSamples.show(10)
  }

  // 定义用户自定义函数（UDF），将整数数组转换为稀疏向量
  val array2vec: UserDefinedFunction = udf { (a: Seq[Int], length: Int) =>
    org.apache.spark.ml.linalg.Vectors.sparse(length, a.sortWith(_ < _).toArray, Array.fill[Double](a.length)(1.0))
  }

  /**
   * Multi-hot 编码示例函数
   * @param samples 电影样本数据集（DataFrame 格式）
   */
  def multiHotEncoderExample(samples: DataFrame): Unit = {
    // 提取电影的每个类型（genre），并生成扁平化的 DataFrame
    val samplesWithGenre = samples.select(
      col("movieId"),
      col("title"),
      explode(split(col("genres"), "\\|").cast("array<string>")).as("genre")
    )

    // 使用 StringIndexer 将 genre 转换为索引值
    val genreIndexer = new StringIndexer().setInputCol("genre").setOutputCol("genreIndex")
    val stringIndexerModel: StringIndexerModel = genreIndexer.fit(samplesWithGenre)

    // 为样本添加 genre 索引值，并将索引值转换为整数类型
    val genreIndexSamples = stringIndexerModel.transform(samplesWithGenre)
      .withColumn("genreIndexInt", col("genreIndex").cast(sql.types.IntegerType))

    // 获取 genre 索引的总数（最大索引值 + 1）
    val indexSize = genreIndexSamples.agg(max(col("genreIndexInt"))).head().getAs   // 将同一电影的所有 genre 索引聚合为列表，并添加向量长度信息
    val processedSamples = genreIndexSamples
      .groupBy(col("movieId"))
      .agg(collect_list("genreIndexInt").as("genreIndexes"))
      .withColumn("indexSize", typedLit(indexSize))

    // 使用自定义函数将 genre 索引列表转换为稀疏向量
    val finalSample = processedSamples.withColumn("vector", array2vec(col("genreIndexes"), col("indexSize")))
    finalSample.printSchema()
    finalSample.show(10)
  }

  // 定义 UDF，将 Double 类型的值转换为密集向量
  val double2vec: UserDefinedFunction = udf { (value: Double) =>
    org.apache.spark.ml.linalg.Vectors.dense(value)
  }

  /**
   * 处理评分样本的特征
   * @param samples 评分样本数据集（DataFrame 格式）
   */
  def ratingFeatures(samples: DataFrame): Unit = {
    samples.printSchema()
    samples.show(10)

    // 计算每部电影的评分计数、平均评分和评分方差
    val movieFeatures = samples.groupBy(col("movieId"))
      .agg(
        count(lit(1)).as("ratingCount"), // 评分次数
        avg(col("rating")).as("avgRating"), // 平均评分
        variance(col("rating")).as("ratingVar") // 评分方差
      )
      .withColumn("avgRatingVec", double2vec(col("avgRating"))) // 将平均评分转换为密集向量

    movieFeatures.show(10)

    // 使用 QuantileDiscretizer 对评分次数进行分桶
    val ratingCountDiscretizer = new QuantileDiscretizer()
      .setInputCol("ratingCount")
      .setOutputCol("ratingCountBucket")
      .setNumBuckets(100) // 分桶数目

    // 使用 MinMaxScaler 对平均评分进行归一化
    val ratingScaler = new MinMaxScaler()
      .setInputCol("avgRatingVec")
      .setOutputCol("scaleAvgRating")

    // 构建处理评分特征的管道
    val pipelineStage: Array[PipelineStage] = Array(ratingCountDiscretizer, ratingScaler)
    val featurePipeline = new Pipeline().setStages(pipelineStage)

    // 应用管道对数据进行处理
    val movieProcessedFeatures = featurePipeline.fit(movieFeatures).transform(movieFeatures)
    movieProcessedFeatures.show(10)
  }

  def main(args: Array[String]): Unit = {
    // 设置日志级别为 ERROR，减少无关日志输出
    Logger.getLogger("org").setLevel(Level.ERROR)

    // 初始化 Spark 配置
    val conf = new SparkConf()
      .setMaster("local") // 本地运行模式
      .setAppName("featureEngineering") // 应用程序名称
      .set("spark.submit.deployMode", "client") // 提交模式为客户端模式

    // 创建 SparkSession
    val spark = SparkSession.builder.config(conf).getOrCreate()

    // 加载电影数据集
    val movieResourcesPath = this.getClass.getResource("/webroot/sampledata/movies.csv")
    val movieSamples = spark.read.format("csv").option("header", "true").load(movieResourcesPath.getPath)
    println("Raw Movie Samples:")
    movieSamples.printSchema()
    movieSamples.show(10)

    // 演示 OneHotEncoder
    println("OneHotEncoder Example:")
    oneHotEncoderExample(movieSamples)

    // 演示 MultiHotEncoder
    println("MultiHotEncoder Example:")
    multiHotEncoderExample(movieSamples)

    // 加载评分数据集并演示评分特征处理
    println("Numerical features Example:")
    val ratingsResourcesPath = this.getClass.getResource("/webroot/sampledata/ratings.csv")
    val ratingSamples = spark.read.format("csv").option("header", "true").load(ratingsResourcesPath.getPath)
    ratingFeatures(ratingSamples)
  }
}

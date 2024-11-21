package com.sparrowrecsys.offline.spark.embedding

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.SetParams

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

object Embedding {

  // Redis 配置，指定 Redis 服务端点和端口
  val redisEndpoint = "localhost"
  val redisPort = 6379

  /**
   * 处理用户的评分序列，生成用户-物品序列
   * @param sparkSession Spark 会话
   * @param rawSampleDataPath 原始评分数据路径
   * @return 用户评分序列的 RDD，序列由物品 ID 构成
   */
  def processItemSequence(sparkSession: SparkSession, rawSampleDataPath: String): RDD[Seq[String]] ={

    // 获取评分数据的文件路径
    val ratingsResourcesPath = this.getClass.getResource(rawSampleDataPath)
    // 加载评分数据文件
    val ratingSamples = sparkSession.read.format("csv").option("header", "true").load(ratingsResourcesPath.getPath)

    // 定义 UDF（用户自定义函数），按时间戳排序物品 ID
    val sortUdf: UserDefinedFunction = udf((rows: Seq[Row]) => {
      rows.map { case Row(movieId: String, timestamp: String) => (movieId, timestamp) }
        .sortBy { case (_, timestamp) => timestamp } // 按时间戳升序排序
        .map { case (movieId, _) => movieId }       // 提取物品 ID
    })

    // 打印数据模式信息
    ratingSamples.printSchema()

    // 筛选评分 >= 3.5 的记录，按用户分组并生成用户的评分序列
    val userSeq = ratingSamples
      .where(col("rating") >= 3.5) // 只保留评分 >= 3.5 的记录
      .groupBy("userId")           // 按用户分组
      .agg(sortUdf(collect_list(struct("movieId", "timestamp"))) as "movieIds") // 按时间排序生成物品序列
      .withColumn("movieIdStr", array_join(col("movieIds"), " ")) // 将物品序列转换为字符串

    // 打印前 10 条用户的评分序列
    userSeq.select("userId", "movieIdStr").show(10, truncate = false)

    // 将物品序列转换为 RDD，返回分割后的序列（Seq[String]）
    userSeq.select("movieIdStr").rdd.map(r => r.getAs[String]("movieIdStr").split(" ").toSeq)
  }

  /**
   * 生成用户的嵌入向量，并选择性地保存到 Redis
   * @param sparkSession Spark 会话
   * @param rawSampleDataPath 原始评分数据路径
   * @param word2VecModel 训练好的 Word2Vec 模型
   * @param embLength 嵌入向量长度
   * @param embOutputFilename 嵌入向量保存文件名
   * @param saveToRedis 是否保存到 Redis
   * @param redisKeyPrefix Redis 键前缀
   */
  def generateUserEmb(sparkSession: SparkSession, rawSampleDataPath: String, word2VecModel: Word2VecModel, embLength:Int, embOutputFilename:String, saveToRedis:Boolean, redisKeyPrefix:String): Unit ={
    val ratingsResourcesPath = this.getClass.getResource(rawSampleDataPath)
    val ratingSamples = sparkSession.read.format("csv").option("header", "true").load(ratingsResourcesPath.getPath)
    ratingSamples.show(10, false)

    // 用户嵌入向量的集合
    val userEmbeddings = new ArrayBuffer[(String, Array[Float])]()

    // 按用户 ID 分组，生成用户嵌入
    ratingSamples.collect().groupBy(_.getAs[String]("userId"))
      .foreach(user => {
        val userId = user._1
        var userEmb = new Array[Float](embLength)

        var movieCount = 0
        // 计算用户的嵌入向量（将物品向量相加求均值）
        userEmb = user._2.foldRight[Array[Float]](userEmb)((row, newEmb) => {
          val movieId = row.getAs[String]("movieId")
          val movieEmb = word2VecModel.getVectors.get(movieId)
          movieCount += 1
          if(movieEmb.isDefined){
            newEmb.zip(movieEmb.get).map { case (x, y) => x + y }
          }else{
            newEmb
          }
        }).map((x: Float) => x / movieCount) // 平均化
        userEmbeddings.append((userId,userEmb))
      })

    // 保存嵌入向量到文件
    val embFolderPath = this.getClass.getResource("/webroot/modeldata/")
    val file = new File(embFolderPath.getPath + embOutputFilename)
    val bw = new BufferedWriter(new FileWriter(file))

    for (userEmb <- userEmbeddings) {
      bw.write(userEmb._1 + ":" + userEmb._2.mkString(" ") + "\n")
    }
    bw.close()

    // 如果保存到 Redis
    if (saveToRedis) {
      val redisClient = new Jedis(redisEndpoint, redisPort)
      val params = SetParams.setParams()
      // 设置 24 小时过期时间
      params.ex(60 * 60 * 24)

      for (userEmb <- userEmbeddings) {
        redisClient.set(redisKeyPrefix + ":" + userEmb._1, userEmb._2.mkString(" "), params)
      }
      redisClient.close()
    }
  }

def trainItem2vec(sparkSession: SparkSession, samples : RDD[Seq[String]], embLength:Int, embOutputFilename:String, saveToRedis:Boolean, redisKeyPrefix:String): Word2VecModel = {
    // 初始化Word2Vec模型，设置向量维度、窗口大小和迭代次数
    val word2vec = new Word2Vec()
      .setVectorSize(embLength)
      .setWindowSize(5)
      .setNumIterations(10)

    // 使用提供的样本训练Word2Vec模型
    val model = word2vec.fit(samples)

    // 示例：打印与"158"相似的前20个词项及其余弦相似度
    val synonyms = model.findSynonyms("158", 20)
    for ((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    // 保存词向量到本地文件
    val embFolderPath = this.getClass.getResource("/webroot/modeldata/")
    val file = new File(embFolderPath.getPath + embOutputFilename)
    val bw = new BufferedWriter(new FileWriter(file))
    for (movieId <- model.getVectors.keys) {
      bw.write(movieId + ":" + model.getVectors(movieId).mkString(" ") + "\n")
    }
    bw.close()

    // 如果设置保存到Redis，则将向量写入Redis，设置TTL为24小时
    if (saveToRedis) {
      val redisClient = new Jedis(redisEndpoint, redisPort)
      val params = SetParams.setParams()
      params.ex(60 * 60 * 24) // TTL为24小时
      for (movieId <- model.getVectors.keys) {
        redisClient.set(redisKeyPrefix + ":" + movieId, model.getVectors(movieId).mkString(" "), params)
      }
      redisClient.close()
    }

    // 执行基于LSH（局部敏感哈希）的嵌入近似搜索
    embeddingLSH(sparkSession, model.getVectors)
    model
  }

  // 单次随机游走的实现
  def oneRandomWalk(transitionMatrix : mutable.Map[String, mutable.Map[String, Double]], itemDistribution : mutable.Map[String, Double], sampleLength:Int): Seq[String] ={
    val sample = mutable.ListBuffer[String]()

    // 选择第一个节点，根据全局分布随机选择
    val randomDouble = Random.nextDouble()
    var firstItem = ""
    var accumulateProb:Double = 0D
    breakable { 
      for ((item, prob) <- itemDistribution) {
        accumulateProb += prob
        if (accumulateProb >= randomDouble){
          firstItem = item
          break
        }
      }
    }

    sample.append(firstItem)
    var curElement = firstItem

    // 根据转移概率矩阵随机选择接下来的节点
    breakable { 
      for(_ <- 1 until sampleLength) {
        if (!itemDistribution.contains(curElement) || !transitionMatrix.contains(curElement)){
          break
        }

        val probDistribution = transitionMatrix(curElement)
        val randomDouble = Random.nextDouble()
        var accumulateProb: Double = 0D
        breakable { 
          for ((item, prob) <- probDistribution) {
            accumulateProb += prob
            if (accumulateProb >= randomDouble){
              curElement = item
              break
            }
          }
        }
        sample.append(curElement)
      }
    }
    Seq(sample.toList : _*)
  }

  // 多次随机游走的实现，生成多个样本
  def randomWalk(transitionMatrix : mutable.Map[String, mutable.Map[String, Double]], itemDistribution : mutable.Map[String, Double], sampleCount:Int, sampleLength:Int): Seq[Seq[String]] ={
    val samples = mutable.ListBuffer[Seq[String]]()
    for(_ <- 1 to sampleCount) {
      samples.append(oneRandomWalk(transitionMatrix, itemDistribution, sampleLength))
    }
    Seq(samples.toList : _*)
  }

  // 生成转移概率矩阵和全局节点分布
  def generateTransitionMatrix(samples : RDD[Seq[String]]): (mutable.Map[String, mutable.Map[String, Double]], mutable.Map[String, Double]) ={
    // 将样本序列转换为相邻对
    val pairSamples = samples.flatMap[(String, String)]( sample => {
      var pairSeq = Seq[(String,String)]()
      var previousItem:String = null
      sample.foreach((element:String) => {
        if(previousItem != null){
          pairSeq = pairSeq :+ (previousItem, element)
        }
        previousItem = element
      })
      pairSeq
    })

    // 统计相邻对的出现次数
    val pairCountMap = pairSamples.countByValue()
    var pairTotalCount = 0L
    val transitionCountMatrix = mutable.Map[String, mutable.Map[String, Long]]()
    val itemCountMap = mutable.Map[String, Long]()

    // 构造转移计数矩阵和节点计数
    pairCountMap.foreach( pair => {
      val pairItems = pair._1
      val count = pair._2

      if(!transitionCountMatrix.contains(pairItems._1)){
        transitionCountMatrix(pairItems._1) = mutable.Map[String, Long]()
      }

      transitionCountMatrix(pairItems._1)(pairItems._2) = count
      itemCountMap(pairItems._1) = itemCountMap.getOrElse[Long](pairItems._1, 0) + count
      pairTotalCount = pairTotalCount + count
    })

    val transitionMatrix = mutable.Map[String, mutable.Map[String, Double]]()
    val itemDistribution = mutable.Map[String, Double]()

    // 计算转移概率矩阵和全局节点分布
    transitionCountMatrix foreach {
      case (itemAId, transitionMap) =>
        transitionMatrix(itemAId) = mutable.Map[String, Double]()
        transitionMap foreach { case (itemBId, transitionCount) => transitionMatrix(itemAId)(itemBId) = transitionCount.toDouble / itemCountMap(itemAId) }
    }

    itemCountMap foreach { case (itemId, itemCount) => itemDistribution(itemId) = itemCount.toDouble / pairTotalCount }
    (transitionMatrix, itemDistribution)
  }

  // 基于局部敏感哈希的嵌入搜索
  def embeddingLSH(spark:SparkSession, movieEmbMap:Map[String, Array[Float]]): Unit ={

    val movieEmbSeq = movieEmbMap.toSeq.map(item => (item._1, Vectors.dense(item._2.map(f => f.toDouble))))
    val movieEmbDF = spark.createDataFrame(movieEmbSeq).toDF("movieId", "emb")

    // 使用LSH模型进行聚类和近似搜索
    val bucketProjectionLSH = new BucketedRandomProjectionLSH()
      .setBucketLength(0.1)
      .setNumHashTables(3)
      .setInputCol("emb")
      .setOutputCol("bucketId")

    val bucketModel = bucketProjectionLSH.fit(movieEmbDF)
    val embBucketResult = bucketModel.transform(movieEmbDF)
    println("movieId, emb, bucketId schema:")
    embBucketResult.printSchema()
    println("movieId, emb, bucketId data result:")
    embBucketResult.show(10, truncate = false)

    println("Approximately searching for 5 nearest neighbors of the sample embedding:")
    val sampleEmb = Vectors.dense(0.795,0.583,1.120,0.850,0.174,-0.839,-0.0633,0.249,0.673,-0.237)
    bucketModel.approxNearestNeighbors(movieEmbDF, sampleEmb, 5).show(truncate = false)
  }

  // 基于图的随机游走生成样本并训练嵌入
  def graphEmb(samples : RDD[Seq[String]], sparkSession: SparkSession, embLength:Int, embOutputFilename:String, saveToRedis:Boolean, redisKeyPrefix:String): Word2VecModel ={
    val transitionMatrixAndItemDis = generateTransitionMatrix(samples)

    println(transitionMatrixAndItemDis._1.size)
    println(transitionMatrixAndItemDis._2.size)

    val sampleCount = 20000
    val sampleLength = 10
    val newSamples = randomWalk(transitionMatrixAndItemDis._1, transitionMatrixAndItemDis._2, sampleCount, sampleLength)

    val rddSamples = sparkSession.sparkContext.parallelize(newSamples)
    trainItem2vec(sparkSession, rddSamples, embLength, embOutputFilename, saveToRedis, redisKeyPrefix)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("ctrModel")
      .set("spark.submit.deployMode", "client")

    val spark = SparkSession.builder.config(conf).getOrCreate()

    val rawSampleDataPath = "/webroot/sampledata/ratings.csv"
    val embLength = 10

    // 加载并处理样本序列数据
    val samples = processItemSequence(spark, rawSampleDataPath)

    // 训练Item2Vec嵌入
    val model = trainItem2vec(spark, samples, embLength, "item2vecEmb.csv", saveToRedis = false, "i2vEmb")
    // 可以启用以下代码以生成基于图的嵌入或用户嵌入
    // graphEmb(samples, spark, embLength, "itemGraphEmb.csv", saveToRedis = true, "graphEmb")
    // generateUserEmb(spark, rawSampleDataPath, model, embLength, "userEmb.csv", saveToRedis = false, "uEmb")
  }
}


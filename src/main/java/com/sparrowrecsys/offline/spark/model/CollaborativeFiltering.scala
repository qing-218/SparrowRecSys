/*代码功能总结
加载数据并预处理：
使用 UDF 将用户 ID、电影 ID 和评分从字符串类型转换为适合建模的数值类型。
模型训练：
使用交替最小二乘法（ALS）构建推荐模型；
设置迭代次数、正则化参数以及冷启动策略。
模型评估：
使用均方根误差（RMSE）评估预测结果；
显示用户和电影的潜在特征向量。
生成推荐：
为所有用户生成推荐；
为指定的用户或电影生成推荐。
超参数调优：
使用 CrossValidator 和 ParamGridBuilder 进行交叉验证；
获取不同正则化参数下的平均性能指标。*/
package com.sparrowrecsys.offline.spark.model

import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CollaborativeFiltering {

  def main(args: Array[String]): Unit = {
    // 配置 Spark
    val conf = new SparkConf()
      .setMaster("local") // 本地运行
      .setAppName("collaborativeFiltering") // 应用名称
      .set("spark.submit.deployMode", "client") // 提交模式为客户端模式

    // 创建 SparkSession
    val spark = SparkSession.builder.config(conf).getOrCreate()

    // 加载评分数据
    val ratingResourcesPath = this.getClass.getResource("/webroot/sampledata/ratings.csv")
    // 定义 UDF，将字符串类型数据转换为 Int 和 Float 类型
    val toInt = udf[Int, String](_.toInt)
    val toFloat = udf[Double, String](_.toFloat)
    val ratingSamples = spark.read.format("csv").option("header", "true").load(ratingResourcesPath.getPath)
      .withColumn("userIdInt", toInt(col("userId"))) // 转换用户 ID 为整数
      .withColumn("movieIdInt", toInt(col("movieId"))) // 转换电影 ID 为整数
      .withColumn("ratingFloat", toFloat(col("rating"))) // 转换评分为浮点数

    // 将数据随机分为训练集（80%）和测试集（20%）
    val Array(training, test) = ratingSamples.randomSplit(Array(0.8, 0.2))

    // 创建 ALS（交替最小二乘法）模型
    val als = new ALS()
      .setMaxIter(5) // 最大迭代次数
      .setRegParam(0.01) // 正则化参数
      .setUserCol("userIdInt") // 用户列
      .setItemCol("movieIdInt") // 项目列（电影 ID）
      .setRatingCol("ratingFloat") // 评分列

    // 在训练数据上训练 ALS 模型
    val model = als.fit(training)

    // 设置冷启动策略为 "drop"，以确保不会产生 NaN 的预测值
    model.setColdStartStrategy("drop")

    // 在测试数据上生成预测结果
    val predictions = model.transform(test)

    // 显示模型的用户和项目因子（潜在特征向量）
    model.itemFactors.show(10, truncate = false)
    model.userFactors.show(10, truncate = false)

    // 使用 RMSE（均方根误差）评估预测结果
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse") // 指标为 RMSE
      .setLabelCol("ratingFloat") // 实际评分列
      .setPredictionCol("prediction") // 预测评分列
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse") // 输出 RMSE

    // 为每个用户生成前 10 部推荐电影
    val userRecs = model.recommendForAllUsers(10)
    // 为每部电影生成前 10 名推荐用户
    val movieRecs = model.recommendForAllItems(10)

    // 为一部分用户生成推荐
    val users = ratingSamples.select(als.getUserCol).distinct().limit(3)
    val userSubsetRecs = model.recommendForUserSubset(users, 10)
    // 为一部分电影生成推荐
    val movies = ratingSamples.select(als.getItemCol).distinct().limit(3)
    val movieSubSetRecs = model.recommendForItemSubset(movies, 10)

    // 展示推荐结果
    userRecs.show(false)
    movieRecs.show(false)
    userSubsetRecs.show(false)
    movieSubSetRecs.show(false)

    // 设置参数网格，用于交叉验证
    val paramGrid = new ParamGridBuilder()
      .addGrid(als.regParam, Array(0.01)) // 测试的正则化参数值
      .build()

    // 创建交叉验证器
    val cv = new CrossValidator()
      .setEstimator(als) // 评估器为 ALS
      .setEvaluator(evaluator) // 评估器为 RMSE 评估器
      .setEstimatorParamMaps(paramGrid) // 参数网格
      .setNumFolds(10) // 10 折交叉验证

    // 在测试集上执行交叉验证
    val cvModel = cv.fit(test)
    val avgMetrics = cvModel.avgMetrics // 获取平均指标分数
    println(s"Cross-validated metrics: ${avgMetrics.mkString(",")}")

    spark.stop() // 停止 SparkSession
  }
}

package com.sparrowrecsys.offline.spark.evaluate

import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.DataFrame

class Evaluator {
  /**
   * 评估模型预测结果的表现
   * @param predictions 包含预测结果的DataFrame，需包含"label"和"probability"列
   */
  def evaluate(predictions: DataFrame): Unit = {

    // 引入隐式转换以便于操作DataFrame
    import predictions.sparkSession.implicits._

    /**
     * 从预测结果中提取得分和标签
     * - "probability" 列为DenseVector类型，获取其第2个元素（正类的预测概率）
     * - "label" 列为真实标签，转为Double类型以便后续计算
     * 结果是一个RDD[(Double, Double)]格式，第一项为预测得分，第二项为真实标签
     */
    val scoreAndLabels = predictions.select("label", "probability").map { row =>
      (row.apply(1).asInstanceOf]("label").toDouble)
    }

    // 使用BinaryClassificationMetrics计算二分类模型的评估指标
    val metrics = new BinaryClassificationMetrics(scoreAndLabels.rdd)

    // 打印PR曲线下面积（AUC-PR），衡量模型在正负样本比例不平衡时的表现
    println("AUC under PR = " + metrics.areaUnderPR())

    // 打印ROC曲线下面积（AUC-ROC），衡量模型整体区分正负样本的能力
    println("AUC under ROC = " + metrics.areaUnderROC())
  }
}

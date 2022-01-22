package day01

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建sc实例
    val conf = new SparkConf()
      .setAppName("wordCount")

    val sc = new SparkContext(conf)

    // 读源
    val tuples = sc.textFile("file:///root/log.log")
      // 转换算子
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      // 操作算子
      .collect()

    print(tuples.toList)


    // 释放资源
    sc.stop()
  }
}

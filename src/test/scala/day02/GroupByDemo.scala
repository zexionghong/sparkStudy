/**
 * @Classname GroupByDemo
 * @Description TODO
 * @Date 2022/1/29 11:54
 * @Created by duizhuang
 */

package day02

import org.apache.spark.{SparkConf, SparkContext}

object GroupByDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("1").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    println(sc.textFile("src/data/core/words.txt")
      .flatMap(_.split(" "))
      .map(v => {
        (v, 1)
      })
//      .groupByKey()
//      .reduceByKey()
      .collect().toList)

    test()

    @deprecated
    def test(): Unit ={

    }





  }

}

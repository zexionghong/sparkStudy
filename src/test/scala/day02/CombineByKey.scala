/**
 * @Classname CombineByKey
 * @Description TODO
 * @Date 2022/1/22 16:11
 * @Created by duizhuang
 */

package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer


object CombineByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CombineByKey").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val listData = new ListBuffer[(Int, Int)]
    listData.append((2,100))
    for (i <- 0 to 10; if i % 2 == 0) {
      listData.append((i,i+1))
    }

    val rdd1: RDD[(Int, Int)] = sc.makeRDD(listData)
      // combineByKey 是最底层的算子，数据在分区内计算再全局聚合一次
      // 第一个参数是对数据的value值要怎么操作的方法
      // 第二个值是对分区内的数据怎么操作，入参第一个值是前面的方法的结果返回值，第二个参数是原始值，结果返回这个方法的返回值再作为第一个值入参，递归
      // 第三个值把各个分区的值加起来
    rdd1.mapPartitionsWithIndex(
      (index:Int,iter:Iterator[(Int, Int)]) =>{
        println(index+s"${iter.toList}")
        iter
      }
    ).collect()

      rdd1
      .combineByKey((x:Int) =>(x,2),(a:(Int,Int),b:Int)=>(a._1+b,a._2),(a:(Int,Int),b:(Int,Int)) =>(a._1+b._1,b._2+a._2) )
      .foreach(println)


    println(listData)
  }


}

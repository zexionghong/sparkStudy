/**
 * @Classname MapWIthIndexDemo
 * @Description TODO
 * @Date 2022/1/22 17:07
 * @Created by duizhuang
 */

package day02

import org.apache.spark.{SparkConf, SparkContext}

object MapWIthIndexDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("mapWithIndex")
    val sc = new SparkContext(conf)

    sc.makeRDD(List(("a",2),("v",2),("s",2),("d",2),("b",2),("as",2),("a",2)))
      .mapPartitionsWithIndex(
        (index:Int,iter:Iterator[(String,Int)])=>{
          println(index+s"${iter.toList}")
          iter
        }
      ).count()
      sc.stop()

  }

}

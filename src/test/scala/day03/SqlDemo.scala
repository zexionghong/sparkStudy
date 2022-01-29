/**
 * @Classname SqlDemo
 * @Description TODO
 * @Date 2022/1/28 14:24
 * @Created by duizhuang
 */

package day03

import org.apache.spark.sql.{DataFrame, SparkSession}

object SqlDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("demo").master("local[2]").config("spark.sql.warehouse.dir", "src/data/sql/employee.json").getOrCreate()
//    val df: DataFrame = spark.read.json("src/data/sql/employee.json")
    val df: DataFrame = spark.read.json("src/data/sql/employee.json")
    println(df.first())
    import spark.implicits._


    spark.stop()



  }

}

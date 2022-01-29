/**
 * @Classname JoinDemo
 * @Description TODO
 * @Date 2022/1/28 14:40
 * @Created by duizhuang
 */

package day03

import org.apache.spark.sql.{DataFrame, SparkSession}

object JoinDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[2]").appName("test").getOrCreate()

    val employee: DataFrame = spark.read.json("src/data/sql/employee.json")
    val department: DataFrame = spark.read.json("src/data/sql/department.json")

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val df: DataFrame = employee.join(department, employee("depId")=== $"id")
    df.show()

    df.groupBy(department("name")).count().show()
    spark.stop()
  }

}

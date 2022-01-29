package day02
import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKey {

  def main(args: Array[String]): Unit = {

    // 实例sc
    val conf: SparkConf = new SparkConf().setAppName("AggregateByKye").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    sc.textFile("src/data/core/score.txt")
      .map((v:String) => {
        val cc: Array[String] = v.split(" ")
        (cc(0),cc(1).toInt)
      })

      // 求和 计算班级合计分值
      // 柯里化，第一个括号是初始值，第二个参数为每个分区内执行的方法，第三个为全局执行的方法
      .aggregateByKey(0)(_+_,_+_)
      .foreach(println)

    sc.stop()








  }



}

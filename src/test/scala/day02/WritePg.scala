/**
 * @Classname WritePg
 * @Description TODO
 * @Date 2022/1/29 10:35
 * @Created by duizhuang
 */

package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

object WritePg {
  def main(args: Array[String]): Unit = {
    // 实例sc
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("1")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD("1,2,3,4,5,6,7,8,9,10".split(","))
    rdd.mapPartitionsWithIndex((index:Int,it:Iterator[String]) =>{
      println(index+""+it.toList)
      it
    }).collect()
    rdd.persist(newLevel = StorageLevel.MEMORY_AND_DISK_SER)
    rdd.foreachPartition(saveWithPartition)
    sc.stop()

  }

  def saveWithPartition(iter: Iterator[String]): Unit = {
    // 实例数据库
    // 创建连接
    val properties = new Properties()
    properties.put("user", "postgres")
    properties.put("password", "postgres")
    properties.put("driver", "org.postgresql.Driver")

    val url = "jdbc:postgresql://10.0.0.50:5433/duizhuang_test"
    val connection: Connection = DriverManager.getConnection(url, properties)
    println(connection)
    // 创建 statement
    val statement: Statement = connection.createStatement()
    val insSql = "INSERT INTO public.user_log( user_id, log_type, create_time, create_id, user_name, param, ip, result, remark, client_type, client_version, client_system_version) VALUES ('wZji1913Hrk1BWd4mZEVP31', '最后登录', '1970-01-19 06:15:40.584321', 'wZji1913Hrk1BWd4mZEVP3', '胡景涛', '{}', '192.168.0.173, 39.108.10.214', '最后登录时间：2019-12-30 21:16:24', '[胡景涛]最后登录时间[2019-12-30 21:16:24]', NULL, NULL, NULL);"
    // 每个分区内部再采集数据，批量写入数据库客户端
    iter.foreach(v => {
      statement.addBatch(insSql)
    })
    // 让客户端提交记录
    statement.executeBatch()
    statement.close()
    connection.close()


  }

}

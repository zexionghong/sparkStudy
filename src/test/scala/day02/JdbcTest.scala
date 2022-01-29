/**
 * @Classname WriteDb
 * @Description TODO
 * @Date 2022/1/29 9:55
 * @Created by duizhuang
 */

package day02

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.Properties

object JdbcTest {
  def main(args: Array[String]): Unit = {

    val prop: Properties = new Properties()
    prop.put("user","postgres")
    prop.put("password","postgres")
    prop.put("driver","org.postgresql.Driver")
    val jdbcUrl: String = "jdbc:postgresql://10.0.0.50:5433/duizhuang_test"
    // 创建连接
    val con: Connection = DriverManager.getConnection(jdbcUrl, prop)

    // 创建statement
    val statement: Statement = con.createStatement()

    // 计数变量
    var count = 0
    for (i <- 1 to 1000){

      val insSql = "INSERT INTO public.user_log( user_id, log_type, create_time, create_id, user_name, param, ip, result, remark, client_type, client_version, client_system_version) VALUES ('wZji1913Hrk1BWd4mZEVP31', '最后登录', '1970-01-19 06:15:40.584321', 'wZji1913Hrk1BWd4mZEVP3', '胡景涛', '{}', '192.168.0.173, 39.108.10.214', '最后登录时间：2019-12-30 21:16:24', '[胡景涛]最后登录时间[2019-12-30 21:16:24]', NULL, NULL, NULL);"
      statement.addBatch(insSql)
      if (count >=102){
        statement.executeBatch()
        count = 0
      }
    }
    statement.executeBatch()

    //    a
    //    val resultSet: ResultSet = statement.executeQuery("select * from user_log")
    //
    //    while (resultSet.next()){
    //      println(resultSet.getInt("id"))
    //    }

    statement.close()
    con.close()




  }

}

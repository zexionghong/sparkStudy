/**
 * @Classname StringToDateTime
 * @Description TODO
 * @Date 2022/1/29 10:40
 * @Created by duizhuang
 */

package day02

import java.sql
import java.text.SimpleDateFormat
import java.util.Date

object StringToDateTime {
  def main(args: Array[String]): Unit = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")

    val date: Date = sdf.parse("1970-01-19 06:15:40.584321")
    val formatStr: String =sdf.format(new Date())
    println(formatStr)
    val birthDate = new java.sql.Timestamp(date.getTime)
    println(birthDate)
  }

}

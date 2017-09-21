package com.min.spark

import java.sql.DriverManager

import scala.util.Random

object MockMysql {
  val url = "jdbc:mysql://192.168.42.1:3306/spark?useUnicode=true&characterEncoding=utf-8"
  val username = "root"
  val password = "123456"
  Class.forName("java.sql.Driver")

  def getConnection() = {
    DriverManager.getConnection(url, username, password)
  }

  def main(args: Array[String]): Unit = {
    val con = getConnection()
    val sql = "insert into user_info (user_id,username,name,age,professional,city) values (?,?,?,?,?,?)"
    val preStatment = con.prepareStatement(sql)
    var random = new Random()
    val familyNames = Array("王", "李", "赵", "刘", "张")
    val firstNames = Array("小", "大", "冉", "飞", "伟", "狗", "浩", "倩", "丽", "云")
    val cities = Array("北京", "深圳", "广州", "上海", "西安", "成都", "重庆", "杭州")
    val profs = Array("程序员", "医生", "老师", "销售", "工人", "公务员", "白领", "无业")
    (1 to 100).foreach(f => {
      preStatment.setLong(1, f)
      preStatment.setString(2, s"account$f")
      preStatment.setString(3, familyNames(random.nextInt(familyNames.length)) + firstNames(random.nextInt(firstNames.length)))
      preStatment.setInt(4, 10 + random.nextInt(50))
      preStatment.setString(5, profs(random.nextInt(profs.length)))
      preStatment.setString(6, cities(random.nextInt(cities.length)))
      preStatment.addBatch()
    })
    preStatment.executeBatch()
    preStatment.close()
    con.close()
  }
}

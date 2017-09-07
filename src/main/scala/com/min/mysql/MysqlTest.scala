package com.min.mysql

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object MysqlTest {
  val conf = new SparkConf().setAppName("dd").setMaster("local")
  val sc = new SparkContext(conf)

  def createCon() = {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
  }

  def main(args: Array[String]): Unit = {
    val mysql = new JdbcRDD(sc, createCon,
      "select * from my_user where uid>=? and uid<=?", 1, 6, 2,
      r => r)
    mysql.foreach(f => println(f.getString(1) + " " + f.getString(2) + " " + f.getString(3) + " " + f.getString(4)))
  }
}

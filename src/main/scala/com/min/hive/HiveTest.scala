package com.min.hive

import org.apache.spark.sql.SparkSession

object HiveTest {
  val spark = SparkSession.builder().appName("ht").master("local").enableHiveSupport().getOrCreate()

  def main(args: Array[String]): Unit = {
    import spark.sql
    spark.sql("create table if not exists spark_hive(col1 Int,col2 String,col3 String) row format delimited fields terminated by ' '")
    val df = sql("select * from spark_hive")
    df.show()
    spark.stop()
  }
}

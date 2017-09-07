package com.min.json

import java.util.Properties

import org.apache.spark.sql.SparkSession

object JSONTest {

  case class JSON(id: String, pid: String, text: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ds").master("local").getOrCreate()
    //read2(spark)
    fromMySql(spark)
  }

  def read(spark: SparkSession) = {
    val df = spark.read.json("src\\main\\scala\\com\\min\\json\\json")
    //import spark.implicits._

    // df.show()
    //df.printSchema()
    //df.select("id").show()
    //df.select($"id",$"pid").show()
    //df.filter($"id" =!= null).show()
    //df.groupBy("id").count().show()

    //sql
    df.createOrReplaceTempView("json")
    val sql = spark.sql("select * from json")
    sql.show()
  }

  def read2(spark: SparkSession) = {
    import spark.implicits._
    //创建dataset
    val ds = Seq(JSON("id", "pid", "text")).toDS()
    //ds.show
    // ds.write.mode(SaveMode.Overwrite).json("d://json")

    val ds2 = Seq(1, 2, 3).toDS()
    //  ds2.map(_+1).collect().foreach(println)

    val ds3 = spark.read.json("src\\main\\scala\\com\\min\\json\\json").as[JSON]
    ds3.show()
  }

  def fromMySql(spark: SparkSession) = {
    val fromDB = spark.read.jdbc("jdbc:mysql://localhost:3306/test?user=root&password=123456", "my_user", new Properties())
    fromDB.show()
  }
}

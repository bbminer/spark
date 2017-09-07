package com.min.sql

import org.apache.spark.sql.SparkSession

object SqlDataTest {

  case class Record(key: Int, value: String)

  def main(args: Array[String]): Unit = {
    //spark sql 逻辑配置
    val spark = SparkSession.builder().appName("sql").master("local").getOrCreate()
    val df = spark.createDataFrame((1 to 100).map(f => {
      //变量+$ 代表引入当前变量的值   字符串前+s 表示说明当前字符串中拿出特殊符号
      Record(f, s"val_$f")
    }))
    //df查询视图，在sql用records的视图名称来查询df的数据
    df.createOrReplaceTempView("records")
    //spark.sql("select * from records").collect().foreach(println)

    val count = spark.sql("select count(*) from records").collect().head.getLong(0)
    //println("all count:" + count)

    val rddFrom = spark.sql("select key,value from records where key <= 10 and key >= 2")
    //rddFrom.rdd.map(r => s"Key:${r(0)},value:${r(1)}").collect().foreach(println)


    import spark.implicits._
  //  df.where($"key" > 60).orderBy($"value".desc).select($"key").collect().foreach(println)

    //文件存储: parquet列式存储
    //df.write.mode(SaveMode.Overwrite).parquet("d:\\lujing")

    val parquetFile=spark.read.parquet("d:\\lujing")
    //$"key" === 60  等于要用3个等号
    parquetFile.where($"key" > 60).select($"key",$"value").collect().foreach(println)

    //二次查询
    parquetFile.createOrReplaceTempView("parquetFile")
    spark.sql("select * from parquetFile").collect().foreach(println)
  }
}

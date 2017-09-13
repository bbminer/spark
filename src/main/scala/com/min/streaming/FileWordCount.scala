package com.min.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

//sparkstreaming
object FileWordCount {
  def main(args: Array[String]): Unit = {
    //流数据对象
    val conf = new SparkConf().setAppName("dsd").setMaster("local")
    val ssc = new StreamingContext(conf, Seconds(5))
    //监听路径，不能指定到文件
    val lines = ssc.textFileStream("src\\main\\scala\\com\\min\\")
    val wordCount = lines.flatMap(f => f.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
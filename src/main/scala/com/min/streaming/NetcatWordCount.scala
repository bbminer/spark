package com.min.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetcatWordCount {
  def main(args: Array[String]): Unit = {
    //跨机器的多线程下处理，在单线程下无法处理
    val conf = new SparkConf().setAppName("dsds").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //从linux的端口获取流数据        ser序列化
    val lines = ssc.socketTextStream("centos131", 6666, StorageLevel.MEMORY_AND_DISK_SER)

    val wc = lines.flatMap(f => f.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    wc.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

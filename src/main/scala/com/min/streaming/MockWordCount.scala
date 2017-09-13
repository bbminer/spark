package com.min.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

//发送
object MockWordCount {
  def main(args: Array[String]): Unit = {
    val rpcClient = new RpcFlumeClient("192.168.42.131", 8888)
    val word = Array[String]("hadoop", "hive", "spring", "map", "reduce", "nima", "zookeeper", "kafka", "flume")
    val random = new Random()
    (1 to 10).foreach(f => {
      var str: String = word(random.nextInt(word.length))
      for (elem <- (1 to 10)) {
        str += " "
        str += word(random.nextInt(word.length))
      }
      rpcClient.send(str)
    })
    rpcClient.close()
  }
}

//接收
object StatWordCount {
  def main(args: Array[String]): Unit = {
    val Array(hostname, port) = Array("192.168.42.1", "23004")
    val conf = new SparkConf().setAppName("re").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("e:\\checkoint")
    val flumeSream = FlumeUtils.createStream(ssc, hostname, port.toInt, StorageLevel.MEMORY_ONLY)
      .map(f => new String(f.event.getBody.array()))
    flumeSream.flatMap(x => x.split(" "))
      .map(f => (f, 1)).reduceByKey(_ + _)
      .updateStateByKey((v: Seq[Int], s: Option[Int]) => {
        val oldStat = s.getOrElse(0)
        var nowAdd = 0
        if (v.length > 0)
          nowAdd = v(0)
        Some(oldStat + nowAdd)
      }).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
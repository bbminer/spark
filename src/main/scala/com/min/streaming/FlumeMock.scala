package com.min.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.Random

object FlumeMock {
  def main(args: Array[String]): Unit = {
    val rpcFlumeClient = new RpcFlumeClient("192.168.42.131", 8888)
    val id = 0
    val familyName = Array("王", "李", "赵", "刘", "张")
    val firstName = Array("小", "大", "冉", "cp", "伟", "狗", "浩")
    val age = 15
    val phone = 88880978
    val classId = Array(101, 102, 103, 104, 105)
    val random = new Random()
    (1 to 100).foreach(f => {
      val nowId = id + f
      val faName = familyName(random.nextInt(5))
      val fiName = firstName(random.nextInt(7))
      val ageT = age + random.nextInt(9)
      val phoneT = phone + random.nextInt(9999)
      val classT = classId(random.nextInt(5))

      rpcFlumeClient.send(s"$nowId|$faName$fiName|$ageT|$phoneT|$classT")
    })
    rpcFlumeClient.close()
  }
}

object Tetst {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val Array(hostname, port) = Array[String]("192.168.42.1", "23004")
    val flumeStream = FlumeUtils.createStream(ssc, hostname, port.toInt, StorageLevel.MEMORY_ONLY).map(f => {
      new String(f.event.getBody.array())
    }).cache()

    val stuStream = stuOp(flumeStream).map(x => {
      val i = x.split("\\|")
      (i(4).toInt, i)
    }).window(Minutes(5))
    //  stuStream.count().print()

    val clsStream = classStu(flumeStream).map(f => {
      val i = f.split("\\|")
      (i(0).toInt, i(1))
    }).window(Minutes(5)) //.print()

    //stuStream  clsStream 合并计算同班的
    clsStream.leftOuterJoin(stuStream)
      .map(x => (x._2._1, x._2._2.getOrElse(null)))
      .map(f => {
        (f._1, if (f._2 != null) 1 else 0)
      })
      .reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }

  def stuOp(flumeStream: DStream[String]) = {
    flumeStream.filter(x => x.split("\\|").length == 5)
  }

  def classStu(flumeStream: DStream[String]) = {
    flumeStream.filter(x => x.split("\\|").length == 2)
  }
}
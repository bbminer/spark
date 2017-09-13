package com.min.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FlumeWordCount {
  val conf = new SparkConf().setAppName("dss").setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(5))

  def main(args: Array[String]): Unit = {
    //push()
    poll()
  }
  /*
  //push的方式从flume里面获取流式数据
  //push的方式从flume里面获取流式数据
  //需要指定flume push的数据的主机和端口
  //push被动的等待flume发送数据

    前提：flume中配置的sink端口是不会自动打开的
    这里配置的hostname和port是帮助flume打开sink端口用的
    先启动当前任务，然后再启动flume的配置（先使用代码将指定的IP下的端口打开
    然后使用flume的配置进行连接)
    连接成功后向flume发送数据，就会通过sources，sink传递向指定的ip下的端口
    当前代码就是打开该IP下的端口等待中的，所以，flume发送过来的全部数据都被
    当前streaming接收并处理*/
  def push(): Unit = {
    val Array(hostname, port) = Array[String]("192.168.42.1", "30000")
    val flumeStreaming = FlumeUtils.createStream(ssc, hostname, port.toInt, StorageLevel.MEMORY_ONLY)
    flumeStreaming.count().print()
    val stringStream = flumeStreaming.map(x => {
      new String(x.event.getBody.toString)
    })
    stringStream.print()
    stringStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()
    ssc.start()
    ssc.awaitTermination()
  }

  /*poll模式处理flume，需要3个jar包
  scala-library-2.11.8.jar
  commons-lang3-3.3.2.jar
  这两个可以从spark的根目录下的jars路径下找到
  spark-streaming-flume-sink_2.11-2.0.1.jar
  这个可以从IDEA的jar库下找到
  找到三个jar包之后将每个jar传递给flume下的lib路径下
  因为flume中自带的有自己的scala版本
  为了防止版本冲突
  将scala-library-2.10.5.jar和commons-lang-2.5.jar
  更改为scala-library-2.10.5.jarbak和commons-lang-2.5.jarbak
 */
  def poll(): Unit = {
    val Array(hostname, port) = Array[String]("192.168.42.131", "43449")
    val flumeStreaming = FlumeUtils.createStream(ssc, hostname, port.toInt, StorageLevel.MEMORY_AND_DISK_SER)
    flumeStreaming.count().print()
//    val stringStream = flumeStreaming.map(x => {
//      new String(x.event.getBody.toString)
//    })
//    stringStream.print()
//    stringStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()
    ssc.start()
    ssc.awaitTermination()
  }
}

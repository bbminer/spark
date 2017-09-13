package com.min.streaming

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object KafkaWordCount {
  def main(args: Array[String]): Unit = {
       read()
   // write()
  }

  //从kafka获取数据流
  def read(): Unit = {
    //1:zookeeper的url  2:消费组名称 3:topics 4:消费者的线程数
    val Array(zkQuorum, group, topics, numThreads) = Array[String]("192.168.42.131:2181", "test1", "min2", "1")
    val conf = new SparkConf().setAppName("kafka").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //设置检查点目录
    ssc.checkpoint("d:\\kafka")
    val topicWC = topics.split(",").map((_, 1)).toMap
    //ssc:当前运行streaming的程序的context
    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, group, topicWC, StorageLevel.MEMORY_ONLY)
    val wordCount = kafkaStream.flatMap(_._2.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //kafkaStream.flatMap(_).print()
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }

  //向kafka写数据流
  def write(): Unit = {
    //1:broker的url  2:topic 3:每秒插入多少行 4:每行多少个单词
    val Array(brokers, topic, msgPerSec, wordPerMsg) = Array[String]("192.168.42.131:9092", "kafkatest", "2", "5")
    val conf = new SparkConf().setAppName("kafka").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    //实例化producter
    val property = new Properties()
    //生产者
    property.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    property.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    property.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](property)
    //用随机数测试
    val random = new Random()
    while (true) {
      //写入多少行
      (1 to msgPerSec.toInt).foreach(f => {
        //每行写入多少单词
        val str = (1 to wordPerMsg.toInt).map(x => random.nextInt(10).toString).mkString(" ")
        val msg = new ProducerRecord[String, String](topic, str)
        println(msg + str)
        producer.send(msg)
      })
      Thread.sleep(3000)
    }
  }
}

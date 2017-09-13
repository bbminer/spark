package com.min.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object StudentTest {
  case class Student(id: Int, name: String, age: Int, phone: String, classId: Int)

  val conf = new SparkConf().setAppName("test").setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(10))
  ssc.checkpoint("e:\\checkoint")

  def main(args: Array[String]): Unit = {
    val Array(hostname, port) = Array[String]("192.168.42.1", "23004")
    val flumeS = FlumeUtils.createStream(ssc, hostname, port.toInt, StorageLevel.MEMORY_AND_DISK)
    val stringS = flumeS.map(f => new String(f.event.getBody.array()))
    //将stringS的数据放入缓存   action操作
    stringS.cache()
    val stuStream = mapStudent(stringS)
    //stuStream.print()
    //stuStream.count().print()

    //countClassId(stuStream)
    // filterAge(stuStream)
    //    avgAge(stuStream)
    //saveStudent(stuStream)
//    winStudent(stuStream)
    winStuClass(stuStream)
    ssc.start()
    ssc.awaitTermination()
  }

  def mapStudent(stringStream: DStream[String]) = {
    stringStream.map(f => {
      val data = f.split("\\|")
      Student(data(0).toInt, data(1), data(2).toInt, data(3), data(4).toInt)
    })
  }

  def countClassId(stringStream: DStream[Student]) = {
    stringStream.map(x => x.classId).countByValue().print()
  }

  def filterAge(stringStream: DStream[Student]) = {
    stringStream.filter(x => x.age == 20).print()
  }

  def avgAge(stringStream: DStream[Student]) = {
    stringStream.map(x => (x.age, 1))
      .reduce((f1, f2) => (f1._1 + f2._1, f1._2 + f2._2))
      .map(x => x._1 * 1.00 / x._2).print()
  }

  def saveStudent(stringStream: DStream[Student]) = {
    stringStream.filter(s => s.name.equals("zhang")).saveAsTextFiles("e:\\student\\student", "txt")
  }

  //统计最近三分钟的数据,每10秒输出一次
  //窗口操作: w:3s s:10s
  def winStudent(stringStream: DStream[Student]) = {
    stringStream.window(Minutes(3), Seconds(10)).count().print()
  }

  def winStuClass(stringStream: DStream[Student]) = {
    stringStream.map(x=>{
      x.classId
    }).countByWindow(Minutes(3),Seconds(10)).print()
  }
}


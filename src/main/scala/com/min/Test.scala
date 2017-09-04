package com.min

import java.io.File
import java.util.Scanner

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object Test {
  val conf = new SparkConf().setAppName("mm").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    wordCount()
    //counts()
   // count()
  }

  def wordCount(): Unit = {
    var scan = new Scanner(new File("E:\\scala\\Nm\\src\\main\\scala\\com\\min\\word"))
    val array = new ArrayBuffer[(String, Int)]()
    while (scan.hasNext) {
      array += ((scan.next(), 1))
    }
    scan.close()
    val word = sc.parallelize(array)
    val count = word.combineByKey(
      (x: Int) => 1,
      (c1: Int, c2: Int) => c1 + c2,
      (y1: Int, y2: Int) => y1 + y2
    )
    //count.saveAsTextFile("e:\\text\\wordCount\\count")
  // count.foreach(println)
    //sort
    val sort = count.sortBy(f => f._2, false)
   // sort.foreach(println)


    val c =count.reduce((x1,x2)=>{
      if (x1._2>x2._2)
        x1
      else
        x2
    })
 // println(c)



    //hot
    val max = sort.first()._2
    val maxWord = sort.filter(f => {
      if (f._2 == max)
        true
      else
        false
    })
    //  maxWord.saveAsTextFile("e:\\text\\wordCount\\maxword")
   maxWord.foreach(println)
  }

  def counts(): Unit = {
    val source = sc.parallelize(Array(("王", 100), ("李", 90), ("张", 80), ("王", 80), ("李", 70), ("张", 90), ("王", 70)))

    //sum
    val sum = source.reduceByKey((x1, x2) => x1 + x2)
    sum.foreach(println)

    //count
    val count = source.combineByKey(
      (x: Int) => 1,
      (c1: Int, c2: Int) => c1 + 1,
      (y1: Int, y2: Int) => y1 + y2
    )
    count.foreach(println)

    //avg
    val avg = source.combineByKey(
      (x: Int) => (x, 1, 1.0),
      (c1: (Int, Int, Double), c2: Int) => (c1._1 + c2, c1._2 + 1, (c1._1 + c2) * 1.0 / (c1._2 + 1)),
      (y1: (Int, Int, Double), y2: (Int, Int, Double)) => ((y1._1 + y2._1), (y1._2 + y2._2), (y1._1 + y2._1) * 1.0 / (y1._2 + y2._2))
    )
    avg.foreach(println)
  }

  def count(): Unit = {
    val lines = sc.textFile("E:\\scala\\Nm\\src\\main\\scala\\com\\min\\log")
    val logRex ="""([\d]+)\s+([a-zA-Z]+)\s+([\d]{1,2})\s+([0-9a-zA-Z]+)\s+([manwoe]+)\s+([\d]{4}/[\d]{1,2}/[\d]{1,2})\s+([0-9]{1,3})\s+([0-9]{1,3})""".r

    class LogCount(val count: Int, val score: Int) extends Serializable {
      def merge(lc: LogCount): LogCount = {
        new LogCount(lc.count + count, lc.score + score)
      }

      override def toString: String = "count:" + count + ",socre:" + score
    }

    def key(line: String): (String, String, String, String) = {
      logRex.findFirstIn(line) match {
        case Some(logRex(id, name, age, _, sex, _, _, score)) => (id, name, age, sex)
        case _ => (null, null, null, null)
      }
    }

    def value(line: String): LogCount = {
      logRex.findFirstIn(line) match {
        case Some(logRex(id, name, age, _, _, _, _, score)) => new LogCount(1, score.toInt)
        case _ => new LogCount(0, 0)
      }
    }

    val stu = lines.map(x => (key(x), value(x))).reduceByKey((y1, y2) => y1.merge(y2))

    val resultSum = stu.map(f => {
      if (f._1._1 != null)
        (f._1._1, ((f._1._2, f._1._3, f._1._4), f._2.score))
    })
    //sum

    resultSum.foreach(println)

    val resultAvg = stu.map(f => {
      if (f._1._1 != null)
        (f._1._1, ((f._1._2, f._1._3, f._1._4), f._2.score * 1.0 / f._2.count))
    })
    //学生avg
    resultAvg.foreach(println)

    //avg学科平均分
    def keyCourt(line: String): String = {
      logRex.findFirstIn(line) match {
        case Some(logRex(id, name, age, _, sex, _, court, score)) => court
        case _ => null
      }
    }

    val avgCourt = lines.map(f => ((keyCourt(f), value(f)))).reduceByKey((y1, y2) => y1.merge(y2))
      .map(f => {
        if (f._1 != null)
          (f._1, f._2.score * 1.0 / f._2.count)
      })
    avgCourt.foreach(println)
  }
}
package com.min

import java.io.File
import java.util.Scanner

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * @author ${user.name}
  */
object App {
  //local模式下   后面可以写线程数[n] n根据CPU核数确定
  //虚拟化worker
  val conf = new SparkConf().setAppName("test").setMaster("local")
  val sc = new SparkContext(conf)


  def main(args: Array[String]) {
    //    mapPar()
    //    mapP()
    //    mapValuesTest()
    //    flatMapTest()
    //    reduceTest()
    //    sortByTest()
    //    sortByKeyTest()
    //    combineByKeyTest()
    //    combineByKeyCount
//    carTest()
    wordCount()
  }

  //mapPartition，对每个分区上的所有元素作为一个集统一映射
  def mapPar(): Unit = {
    val rdd = sc.parallelize(1 to 9, 3)
    val pm = rdd.mapPartitions(f => {
      var re = List[(Int, Int)]()
      for (i <- f) {
        re = (i, f.hashCode()) :: re
      }
      re.iterator
    })
    pm.foreach(println)
  }

  def mapP(): Unit = {
    val rdd = sc.parallelize(1 to 9, 3)
    var psum = rdd.mapPartitions(r => {
      var result = List[Int]()
      var sum = 0
      for (i <- r) {
        sum += i
      }
      result = sum :: result
      result.iterator
    })
    psum.foreach(println)
  }

  def mapValuesTest(): Unit = {
    val a = sc.parallelize(Array("a", "bb", "vvv", "d"), 2)
    val p = a.map(x => (x.length, x))
    val mapValues = p.mapValues(x => x + "--")
    mapValues.foreach(println)
  }

  def flatMapTest(): Unit = {
    val a = sc.parallelize(Array((1, "had oop"), (2, "ja va"), (3, "mm p"), (1, "sp ark"), (2, "mmp")))
    val fmv = a.flatMapValues(s => s.split(" "))
    fmv.foreach(println)
  }

  def reduceTest(): Unit = {
    val a = sc.parallelize(Array("a", "b", "c", "d", "e", "f", "g", "h"))
    val r = a.reduce((s1, s2) => s1 + s2)
    println(r)
  }

  def sortByTest(): Unit = {
    val a = sc.parallelize(Array(1, 2, 7, 6, 3, 72, 54))
    //false 代表降序排列,反之升序
    val s = a.sortBy(x => x, false)
    /*
    //隐式转换，自定义排序规则
    implicit val its=new Ordering[Int] {
   override def compare(x: Int, y: Int): Int = {
        x.toString().compareTo(y.toString)
      }
    }
    val s = a.sortBy(x => x.toString(), false)
    */
    s.foreach(println)
  }

  def sortByKeyTest(): Unit = {
    val a = sc.parallelize(Array("a", "d", "er"))
    val b = sc.parallelize(Array("f", "g", "yu"))
    val c = b.zip(a)
    c.foreach(println)
    val d = c.sortByKey()
    d.foreach(println)
  }

  def combineByKeyTest(): Unit = {
    val a = sc.parallelize(Array((1, "a"), (2, "b"), (3, "b"), (2, "h")))
    val b = a.combineByKey[String]((s: String) => s, (c: String, s: String) => s + c,
      (c1: String, c2: String) => c1 + c2)
    b.foreach(println)
  }

  def combineByKeyCount(): Unit = {
    val a = sc.parallelize(Array(("a", 12), ("b", 7), ("a", 4), ("v", 8), ("b", 0), ("b", 8)))
    val b = a.combineByKey((x: Int) => (x, 1.0),
      (x1: (Int, Double), x2: Int) => (x1._1 + x2, (x1._1 + x2) * 1.0 / (x1._2 + 1)),
      (c1: (Int, Double), c2: (Int, Double)) => (c1._1 + c2._1, c1._2 + c2._2)
    )
    b.foreach(println)
  }

  def carTest(): Unit = {
    val a = sc.parallelize(Array(1, 2, 3))
    val b = sc.parallelize(Array(4, 5, 6))
    val c = a.cartesian(b)
    c.foreach(println)
  }

  def wordCount(): Unit = {
    var scan = new Scanner(new File("src\\main\\scala\\com\\min\\word"))
    var array = new ArrayBuffer[(String, Int)]
    while (scan.hasNext()) {
      array += ((scan.next(), 1))
    }
    scan.close()
    val w = sc.parallelize(array)
    val b = w.combineByKey((x: Int) => 1,
      (x1: Int, x2: Int) => x1 + x2,
      (y1: Int, y2: Int) => y1 + y2
    )
    b.foreach(println)
  }
}
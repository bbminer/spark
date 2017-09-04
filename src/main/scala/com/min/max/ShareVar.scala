package com.min.max

import org.apache.spark.{SparkConf, SparkContext}

object ShareVar {
  val conf = new SparkConf().setAppName("ds").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    //    accumulatorTest()
    broadCastTest()
  }

  def accumulatorTest(): Unit = {
    //定义累加器bcount
    val bcount = sc.longAccumulator("bcount")
    val file = sc.textFile("src\\main\\scala\\com\\min\\word")
    file.map(x => {
      if (x != "" && x != null) bcount.add(1);
      println("bcount")
    }).collect() //不加 .collect() 累加器的值为0
    println("bcount:" + bcount)
  }

  //广播
  def broadCastTest(): Unit = {
    //广播因为做缓存处理，所以读取的表比较小，后面多次使用，就不用去库里拿
    val test = sc.broadcast[List[(Int, String)]](List((1, "name1"), (2, "name2"), (3, "name3"), (4, "name4")))
    val file = sc.textFile("src\\main\\scala\\com\\min\\max\\test")
    val result = file.map(f => f.split(" ")).map(x => {
      val name = test.value.filter(p => p._1 == x(0).toInt)
      val r = if (name.size > 0) name(0)._2 else "no name"
      (x, r)
    }).map(y => y._2 :: y._1.toList).collect()
    result.foreach(println)
  }
}

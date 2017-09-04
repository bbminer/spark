package com.min.max

import org.apache.spark.{SparkConf, SparkContext}

object ShareVar {
  val conf = new SparkConf().setAppName("ds").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    accumulatorTest()
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
}

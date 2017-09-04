package com.min

import org.apache.spark.{SparkConf, SparkContext}

object APIJoin {
  val conf = new SparkConf().setAppName("d").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    joinTest()
  }

  def joinTest(): Unit = {
    val stu = sc.parallelize(Array((1, ("网吧", "男", 12)), (2, ("小鱼", "女", 13)), (3, ("王你妈", "男", 18)), (4, ("啦啦阿", "女", 16))))
    val cor = sc.parallelize(Array((1, "语文"), (2, "数学"), (3, "英语")))
    val scr = sc.parallelize(Array((1, 2, 33), (2, 2, 55), (3, 2, 66), (4, 1, 77), (1, 1, 99), (1, 2, 19), (1, 3, 80)))

    val stuScr = scr.map(x => ((x._1), (x._2, x._3)))
    val scrs = stu.join(stuScr) //相同key组成元组   ( ,(,,),(,))
    val vJoin = scrs.map(x => (x._2._2._1, x)).join(cor) //(,(( ,(,,),(,)),))
    val avg = vJoin.map(x => (x._2._1._1, x._2._1._2._2._2)).groupByKey()
      .map(f => (f._1, f._2.sum * 1.00 / f._2.size)).join(stu)
    avg.foreach(println)
  }
}

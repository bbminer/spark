package com.min.spark

import java.text.SimpleDateFormat

import com.min.spark.SessionFat.UserVisitAction
import org.apache.spark.{SparkConf, SparkContext}

object SessionCateage {
  val conf = new SparkConf().setAppName("spark cat").setMaster("local[2]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val sessions = sc.textFile("hdfs://192.168.42.131:9000/useraction/2017-9-19")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val sessionRdd = sessions.map(f => {
      val infos = f.split("\\|")
      UserVisitAction(infos(0).split(" ")(0), infos(1), infos(2), infos(3), dateFormat.parse(infos(0)),
        if (infos(4).equals("搜索")) infos(7) else "",
        if (infos(4).equals("查看")) infos(5) else "", if (infos(4).equals("查看")) infos(6) else "",
        if (infos(4).equals("支付")) infos(5) else "", if (infos(4).equals("支付")) infos(6) else "",
        if (infos(4).equals("下单")) infos(5) else "", if (infos(4).equals("下单")) infos(6) else ""
      )
    })
    val result = sessionRdd.map(x => (x.clickCateageId, x.sessionId)).filter(f => f._1 != "").map(v => ((v._1, v._2), 1)).reduceByKey(_ + _)
    result.sortBy(f => f._2, false).foreach(println)
  }
}

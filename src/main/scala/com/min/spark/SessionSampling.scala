package com.min.spark

import java.text.SimpleDateFormat

import com.min.spark.SessionFat.UserVisitAction
import org.apache.spark.{SparkConf, SparkContext}

/*
session抽样
 */
object SessionSampling {
  val conf = new SparkConf().setAppName("spark sam").setMaster("local[2]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val sessions = sc.textFile("hdfs://192.168.42.131:9000/useraction/2017-9-19")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val sessionRdd = sessions.map(f => {
      val infos = f.split("\\|")
      UserVisitAction(infos(0).split(" ")(0), infos(1), infos(2), infos(3), dateFormat.parse(infos(0)),
        if (infos(4).equals("搜索")) infos(7) else "",
        if (infos(4).equals("查看")) infos(5) else "", if (infos(4).equals("查看")) infos(6) else "",
        if (infos(4).equals("下单")) infos(5) else "", if (infos(4).equals("下单")) infos(6) else "",
        if (infos(4).equals("支付")) infos(5) else "", if (infos(4).equals("支付")) infos(6) else ""
      )
    })
    val startTime = dateFormat.parse("2017-9-19 0:0:0").getTime
    val hourTime = 60 * 60 * 1000
    val sessionStaticRdd = sessionRdd.map(f => (f.sessionId, f)).aggregateByKey((0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))(
      (u, v) => {
        val actionTime = v.actionTime.getTime
        (u._1 + (if (actionTime >= startTime && actionTime <= (startTime + hourTime * 2)) 1 else 0),
          u._2 + (if (actionTime >= (startTime + hourTime * 2) && actionTime <= (startTime + hourTime * 4)) 1 else 0),
          u._3 + (if (actionTime >= (startTime + hourTime * 4) && actionTime <= (startTime + hourTime * 6)) 1 else 0),
          u._4 + (if (actionTime >= (startTime + hourTime * 6) && actionTime <= (startTime + hourTime * 8)) 1 else 0),
          u._5 + (if (actionTime >= (startTime + hourTime * 8) && actionTime <= (startTime + hourTime * 10)) 1 else 0),
          u._6 + (if (actionTime >= (startTime + hourTime * 10) && actionTime <= (startTime + hourTime * 12)) 1 else 0),
          u._7 + (if (actionTime >= (startTime + hourTime * 12) && actionTime <= (startTime + hourTime * 14)) 1 else 0),
          u._8 + (if (actionTime >= (startTime + hourTime * 14) && actionTime <= (startTime + hourTime * 16)) 1 else 0),
          u._9 + (if (actionTime >= (startTime + hourTime * 16) && actionTime <= (startTime + hourTime * 18)) 1 else 0),
          u._10 + (if (actionTime >= (startTime + hourTime * 18) && actionTime <= (startTime + hourTime * 20)) 1 else 0),
          u._11 + (if (actionTime >= (startTime + hourTime * 20) && actionTime <= (startTime + hourTime * 22)) 1 else 0),
          u._12 + (if (actionTime >= (startTime + hourTime * 22)) 1 else 0), u._13 + 1)
      },
      (v1, v2) => {
        (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3, v1._4 + v2._4,
          v1._5 + v2._5, v1._6 + v2._6, v1._7 + v2._7, v1._8 + v2._8,
          v1._9 + v2._9, v1._10 + v2._10, v1._11 + v2._11, v1._12 + v2._12, v1._13 + v2._13)
      })
    val s = sessionStaticRdd.map(f => f._2).reduce((v1, v2) => {
      (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3, v1._4 + v2._4,
        v1._5 + v2._5, v1._6 + v2._6, v1._7 + v2._7, v1._8 + v2._8,
        v1._9 + v2._9, v1._10 + v2._10, v1._11 + v2._11, v1._12 + v2._12, v1._13 + v2._13)
    })
    println(s)
  }
}

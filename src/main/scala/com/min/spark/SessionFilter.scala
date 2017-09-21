package com.min.spark

import java.text.SimpleDateFormat
import java.util.Date

import com.min.spark.SessionFat.UserVisitAction
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/*
筛选用户
 */
object SessionFilter {
  def main(args: Array[String]): Unit = {
    val conn = MockMysql.getConnection()
    val sql = "select user_id from user_info where age >= 30 and age <= 50"
    val result = conn.createStatement().executeQuery(sql)

    val list = ListBuffer[String]()
    while (result.next()) {
      list += result.getLong("user_id").toString
    }
    //println(list)
    //list转rdd
    val conf = new SparkConf().setAppName("dsds").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val userRdd = sc.parallelize(list)
    val logRdd = sc.textFile("hdfs://192.168.42.131:9000/useraction/2017-9-19")

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val userVisitActionRdd = logRdd.map(f => {
      val infos = f.split("\\|")
      val date = dateFormat.parse(infos(0))
      //       时间                         userId sessionId
      UserVisitAction(infos(0).split(" ")(0), infos(1), infos(2),
        //pageId  时间               搜索关键字
        infos(3), date, if (infos(4).equals("搜索")) infos(7) else "",
        //                clickProductId                     clickCateageId
        if (infos(4).equals("查看")) infos(5) else "", if (infos(4).equals("查看")) infos(6) else "",
        //                orderCategoryId                    orderProductId
        if (infos(4).equals("下单")) infos(5) else "", if (infos(4).equals("下单")) infos(6) else "",
        //                payProductId                        payCategoryId
        if (infos(4).equals("支付")) infos(5) else "", if (infos(4).equals("支付")) infos(6) else "")
    })
    //userVisitActionRdd.foreach(println)
    //聚合join
    val idUserKey = userRdd.map(f => (f, ""))
    val idVisitKey = userVisitActionRdd.map(f => (f.userId, f))
    val join = idUserKey.leftOuterJoin(idVisitKey).filter(f => f._2._2 != None).map(x => x._2._2.get)
    //session步长
    val nowDate = new Date()
    val sessionStaticRdd = join.map(f => {
      ((f.actionDate, f.sessionId, f.userId), f)
      //按照key聚合       最小时间  最大时间   pageid集合
    }).aggregateByKey((nowDate, nowDate, List[String]()))(
      //uval:同key下的value   z:初始值
      (z, uval) => {
        (if (z._1.getTime == nowDate.getTime) uval.actionTime else {
          if (z._1.getTime > uval.actionTime.getTime) uval.actionTime else z._1
        },
          if (z._1.getTime == nowDate.getTime) uval.actionTime else {
            if (z._1.getTime < uval.actionTime.getTime) uval.actionTime else z._1
          },
          if (z._3.size == 0) uval.pageId :: Nil else uval.pageId :: z._3)
      }, (uval1, uval2) => {
        (if (uval1._1.getTime > uval2._1.getTime) uval2._1 else uval1._1,
          if (uval1._2.getTime < uval2._2.getTime) uval2._2 else uval1._2,
          uval1._3 ++ uval2._3)
      }).mapValues(f => {
      ((f._2.getTime - f._1.getTime) / 1000, f._3.distinct.size)
    })

    //统计session
    val actionStaticRdd = sessionStaticRdd.map(f => {
      //时间     访问时长   步长
      (f._1._1, (f._2._1, f._2._2))
    }).aggregateByKey((0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))(
      //1s~3s、4s~6s、7s~9s
      // 10s~30s、30s~60s、1m~3m
      // 3m~10m、10m~30、30m+
      // 1~3、4~6、7~9、
      // 10~30、30~60、60以上各个范围内的session占比
      (u, v) => {
        //时长
        (u._1 + (if (v._1 <= 3) 1 else 0),
          u._2 + (if (v._1 >= 4 && v._1 <= 6) 1 else 0),
          u._3 + (if (v._1 >= 7 && v._1 <= 9) 1 else 0),
          u._4 + (if (v._1 >= 10 && v._1 <= 30) 1 else 0),
          u._5 + (if (v._1 >= 30 && v._1 <= 60) 1 else 0),
          u._6 + (if (v._1 > 60 && v._1 <= 60 * 3) 1 else 0),
          u._7 + (if (v._1 > 60 * 3 && v._1 <= 60 * 10) 1 else 0),
          u._8 + (if (v._1 > 60 * 10 && v._1 <= 60 * 30) 1 else 0),
          u._9 + (if (v._1 > 60 * 30) 1 else 0),
          //步长
          u._10 + (if (v._2 <= 3) 1 else 0),
          u._11 + (if (v._2 >= 4 && v._2 <= 6) 1 else 0),
          u._12 + (if (v._2 >= 7 && v._2 <= 9) 1 else 0),
          u._13 + (if (v._2 >= 10 && v._2 <= 30) 1 else 0),
          u._14 + (if (v._2 >= 30 && v._2 <= 60) 1 else 0),
          u._15 + (if (v._2 > 60) 1 else 0),
          u._16 + 1) //总人数+1
      }
      , (v1, v2) => {
        (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3, v1._4 + v2._4,
          v1._5 + v2._5, v1._6 + v2._6, v1._7 + v2._7, v1._8 + v2._8,
          v1._9 + v2._9, v1._10 + v2._10, v1._11 + v2._11, v1._12 + v2._12,
          v1._13 + v2._13, v1._14 + v2._14, v1._15 + v2._15, v1._16 + v2._16
        )
      }
    )

    val results = actionStaticRdd.take(1).map(f => {
      (f._1, (s"${f._2._1 * 1.0f / f._2._16 * 100}%",
        s"${f._2._2 * 1.0f / f._2._16 * 100}%", s"${f._2._3 * 1.0f / f._2._16 * 100}%",
        s"${f._2._4 * 1.0f / f._2._16 * 100}%", s"${f._2._5 * 1.0f / f._2._16 * 100}%",
        s"${f._2._6 * 1.0f / f._2._16 * 100}%", s"${f._2._7 * 1.0f / f._2._16 * 100}%",
        s"${f._2._8 * 1.0f / f._2._16 * 100}%", s"${f._2._9 * 1.0f / f._2._16 * 100}%",
        s"${f._2._10 * 1.0f / f._2._16 * 100}%", s"${f._2._11 * 1.0f / f._2._16 * 100}%",
        s"${f._2._12 * 1.0f / f._2._16 * 100}%", s"${f._2._13 * 1.0f / f._2._16 * 100}%",
        s"${f._2._14 * 1.0f / f._2._16 * 100}%", s"${f._2._15 * 1.0f / f._2._16 * 100}%"
      ))
    })
    results.foreach(println)
  }
}

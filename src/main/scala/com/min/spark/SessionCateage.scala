package com.min.spark

import java.io.Serializable
import java.text.SimpleDateFormat
import com.min.spark.SessionFat.UserVisitAction
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

object SessionCateage {

  def main(args: Array[String]): Unit = {
    val conn = MockMysql.getConnection()
    val sql = "select user_id from user_info where age >= 30 and age <= 50"
    val result = conn.createStatement().executeQuery(sql)

    val list = ListBuffer[String]()
    while (result.next()) {
      list += result.getLong("user_id").toString
    }
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

    val result41 = join.map(f => (f.clickCateageId, f.sessionId)).filter(_._1 != "")
      .map(x => (x._1, 1)).reduceByKey(_ + _).sortBy(f => f._2, false)
    // result41.take(10).foreach(println)

    val result42 = join.map(f => (f.orderCategoryId, f.sessionId)).filter(_._1 != "")
      .map(x => (x._1, 1)).reduceByKey(_ + _).sortBy(f => f._2, false)
    //result42.take(10).foreach(println)

    val result43 = join.map(f => (f.payCategoryId, f.sessionId)).filter(_._1 != "")
      .map(x => (x._1, 1)).reduceByKey(_ + _).sortBy(f => f._2, false)
    // result43.take(10).foreach(println)

    val re =join.map(f => (f.payCategoryId, f.sessionId))

    val result5 = join.map(f => (f.clickCateageId, f.sessionId)).filter(_._1 != "")
      .map(x => (x, 1)).reduceByKey(_ + _).sortBy(v => v._2, false)
    // result5.take(10).foreach(println)

    class HotProduct() extends Ordering[Tuple3[Int, Int, Int]] with Serializable {
      override def compare(x: (Int, Int, Int), y: (Int, Int, Int)): Int = {
        val com = x._1.compareTo(y._1)
        if (com == 0) {
          val com1 = x._2.compareTo(y._2)
          if (com1 == 0) {
            val com3 = x._3.compareTo(y._3)
            if (com3 == 0) x._3 else com3
          }
          else com1
        }
        else com
      }
    }
  }
}

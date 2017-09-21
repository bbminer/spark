package com.min.spark

import java.util.UUID

import com.min.streaming.RpcFlumeClient

import scala.util.Random

/*
用户行为数据
 */
object MockUserVisitLog {
  def main(args: Array[String]): Unit = {
    val (hostname, port) = ("192.168.42.131", 8888)
    val flumeClient = new RpcFlumeClient(hostname, port)
    val random = new Random()
    var i = 0
    while (i < 1200) {
      val userId = 1 + random.nextInt(100)
      val sessionId = UUID.randomUUID()
      val actionTypes = Array("搜索", "查看", "下单", "支付")
      val keyWord = Array("美食", "水果", "蔬菜", "化妆品", "凉席", "手机", "电脑", "空调", "其他")
      val hour = random.nextInt(24)
      val minute = random.nextInt(60)

      (1 to random.nextInt(50)).foreach(f => {
        val second = random.nextInt(60)
        //一分钟操作超过20次，有问题的数据
        val nowMin = if (f > 20) minute + random.nextInt(10) else minute
        val time = s"2017-9-19 $hour:${if (nowMin > 60) nowMin - 60 else nowMin}:$second"
        val pageId = random.nextInt(50) + 1
        val proudctId = random.nextInt(100)
        val cateageId = random.nextInt(10)
        val actionType = actionTypes(random.nextInt(4))
        val result =s"""$time|$userId|$sessionId|$pageId|$actionType|$proudctId|$cateageId|${if (actionType.equals("搜索")) keyWord(random.nextInt(keyWord.length)) else null}"""
        println(result)
        flumeClient.send(result)
      })
      i = i + 1
    }
    flumeClient.close()
  }
}

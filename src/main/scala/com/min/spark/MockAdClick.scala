package com.min.spark

import java.util.Date

import com.min.streaming.RpcFlumeClient

import scala.util.Random

object MockAdClick {
  def main(args: Array[String]): Unit = {
    val (hostname,port) = ("192.168.65.128",8888)
    val flumeClient = new RpcFlumeClient(hostname,port)

    val area = Array(("北京","北京"),("河南","郑州"),("江苏","南京"),("云南","昆明"),("湖北","武汉")
      ,("湖南","长沙"),("山东","青岛"),("重庆","重庆"))
    val random = new Random()
    while(true){
      val userId = 1 + random.nextInt(100)//(1-100)//用户ID
      val timestamp = new Date().getTime//模拟点击时间
      val adId = 1 + random.nextInt(30)//广告ID
      val areaNow = area(random.nextInt(8))//用户所在地

      val adClickLog = s"""${timestamp}|${userId}|${adId}|${areaNow._1}|${areaNow._2}"""

      //用来产生黑名单
      flumeClient.send(adClickLog)
      Thread.sleep(1000)
    }
//    flumeClient.closeClient()
  }
}
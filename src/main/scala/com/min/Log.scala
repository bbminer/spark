package com.min

import org.apache.spark.{SparkConf, SparkContext}

object Log {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sd").setMaster("local")
    val sc = new SparkContext(conf)

    val file = sc.textFile("e:\\userlog.txt")
    val logR ="""([\d]+)\s+([a-zA-Z]+)\s+([0-9a-zA-Z]+)\s+([manwoe]+)\s+([\d]{4}/[\d]{1,2}/[\d]{1,2})\s+([0-9]{1,3})\s+([0-9]{1,3})""".r
    class Stat(val count: Int, val num: Int) extends Serializable {
      def merge(other: Stat): Stat = {
        new Stat(count + other.count, num + other.num)
      }

      override def toString: String = {
        "count:" + count + ",num:" + num
      }
    }

    def exCom(line: String): (String, String) = {
      logR.findFirstIn(line) match {
        case Some(logR(id, name, _, _, _, _, sorce)) =>(id, name)
        case _ =>(null, null)
      }
    }

    def eValue(line: String): Stat = {
      logR.findFirstIn(line) match {
        case Some(logR(id, name, _, _, _, _, sorce)) => new Stat(1, sorce.toInt)
        case _ => new Stat(0, 0)
      }
    }

    val re = file.map(line => (exCom(line), eValue(line))).reduceByKey((x1, x2) => x1.merge(x2))

    re.saveAsTextFile("e:\\output")
  }
}

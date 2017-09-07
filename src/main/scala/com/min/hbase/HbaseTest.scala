package com.min.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

object HbaseTest {
  val conf = new SparkConf().setAppName("hbase").setMaster("local")
  val sc = new SparkContext(conf)
  val hbaseConf = HBaseConfiguration.create()
  val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
  val table = hbaseConn.getTable(TableName.valueOf("student"))

  def main(args: Array[String]): Unit = {
    /*
    读
     */
    // read()
    //write()
    //test()
    test2()
  }

  def read() = {
    val hConf = HBaseConfiguration.create()
    //增加配置,读取表的内容
    hConf.set(TableInputFormat.INPUT_TABLE, "student")
    val hbaseRDD = sc.newAPIHadoopRDD(hConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val rowRDD = hbaseRDD.map(x => (Bytes.toString(x._1.get()), Bytes.toString(x._2.getValue("s".getBytes, null))))
    println(rowRDD.count())
    rowRDD.foreach(println)
    sc.stop()
  }

  //单个写入
  def write() = {
    //实现完全和parallize一致，另外可以增加位置信息
    val rdd = sc.makeRDD(Array(1)).flatMap(x => 0 to 100)
    rdd.foreachPartition(f => {
      f.foreach(v => {
        var put = new Put(Bytes.toBytes(v.toString))
        put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("write"), Bytes.toBytes(v.toString))
        table.put(put)
      })
    })
  }

  //批量写入
  def test() = {
    var rdd = sc.makeRDD(Array(1)).flatMap(x => 0 to 100)
    rdd.map(v => {
      var put = new Put(Bytes.toBytes(v))
      put.addColumn(Bytes.toBytes("s"), Bytes.toBytes("write"), Bytes.toBytes(v))
      put
    }).foreachPartition(f => {
      //转换
      import scala.collection.JavaConversions._
      table.put(seqAsJavaList(f.toSeq))
    })
  }

  //存到hadoop
  def test2() = {
    var jobConf = new JobConf(hbaseConf)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "student")
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    var rdd = sc.makeRDD(Array(1)).flatMap(x => 0 to 100)
    rdd.map(f => {
      var put = new Put(Bytes.toBytes(f))
      put.addColumn(Bytes.toBytes("s"), Bytes.toBytes("testw"), Bytes.toBytes(f))
      (new ImmutableBytesWritable, put)
    }).saveAsHadoopDataset(jobConf)
  }
}

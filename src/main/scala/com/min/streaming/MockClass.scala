package com.min.streaming

object MockClass {
  def main(args: Array[String]): Unit = {
    val rpcFlumeClient = new RpcFlumeClient("192.168.42.131", 8888)
    val classes = Array("101|物理", "102|生物", "103|数学", "104|语文", "105|历史")
    classes.foreach(f => rpcFlumeClient.send(f))
    rpcFlumeClient.close()
  }
}

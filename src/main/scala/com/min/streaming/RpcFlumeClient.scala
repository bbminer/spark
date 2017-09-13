package com.min.streaming

import java.nio.charset.Charset

import org.apache.flume.api.RpcClientFactory
import org.apache.flume.event.EventBuilder

class RpcFlumeClient(val hostname: String, val port: Int) {
  val client = RpcClientFactory.getDefaultInstance(hostname, port)

  def send(data: String) = {
    val event = EventBuilder.withBody(data, Charset.forName("utf-8"))
    client.append(event)
  }

  def close(): Unit = {
    if (client != null)
      client.close()
  }
}

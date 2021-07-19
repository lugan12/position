package com.lugan.flink.calc

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object Demo03 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataDS: DataStream[String] = env.socketTextStream("localhost",7777)
    dataDS.print()
    env.execute()
  }

}

package com.lugan.flink.calc

//import com.lugan.flink.util.MyKafkaUtil
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//
//object demo01 {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)
//
//     env.addSource(MyKafkaUtil.getConsumer("test"))
//
//    env.execute()
//  }
//}

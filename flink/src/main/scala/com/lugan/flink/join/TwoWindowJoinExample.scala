package com.lugan.flink.join

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object TwoWindowJoinExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream1: DataStream[(String, Int)] = env.fromElements(("a",1000),("a",2000),("b",1000),("b",2000))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Int)](Time.seconds(5)) {
        override def extractTimestamp(element: (String, Int)): Long = element._2
      })

    val stream2: DataStream[(String, Int)] = env.fromElements(("a", 1000), ("a", 2000), ("b", 1000), ("b", 2000))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Int)](Time.seconds(5)) {
        override def extractTimestamp(element: (String, Int)): Long = element._2
      })
    /**
      * 在相同的window窗口中所有数据，stream1和stream2中key相同的元素进行匹配
      */
    stream1.join(stream2).where(t=>t._1)
      .equalTo(t=>t._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .apply(new SimpleJoin).print("rs:")
    env.execute()
  }

}
class SimpleJoin extends JoinFunction[(String, Int),(String, Int),String]{
  override def join(first: (String, Int), second: (String, Int)): String = {
    first +"=>"+second
  }
}

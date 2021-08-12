package com.lugan.flink.join

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment,_}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


/**
  * join 最近10分钟的数据
  *intervalJoin 应用于 keyedStream，当在两个数据流乱序或者延迟时，
  * 可以用相同的key将数据放在一段指定的时间between(lowbound, highbound)内进行连接
  */
object IntervalJoinExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val keyedStream1: KeyedStream[(String, Long, String), String] = env.fromElements(("user_1", 1000L * 80, "click"),("user_1", 1000L * 20, "click"))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, String)](Time.seconds(5)) {
        override def extractTimestamp(element: (String, Long, String)): Long = element._2
      })
      .keyBy(_._1)

    val keyedStream2: KeyedStream[(String, Long, String), String] = env.fromElements(("user_1", 1000L * 4, "browse"),("user_1", 1000L * 80, "browse"), ("user_1", 1000L * -11, "browse"))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, String)](Time.seconds(5)) {
        override def extractTimestamp(element: (String, Long, String)): Long = element._2
      }).keyBy(_._1)
    /**模版：stream1.intervalJoin(stream2).between(lowerBound,upperBound).process(new ProcessJoinFunction)
      *2. 时间戳范围条件 ： stream1.timestamp ∈ [stream2.timestamp + lowerBound; stream2.timestamp + upperBound]  stream2.timestamp + lowerBound <= stream1.timestamp and stream1.timestamp <= stream2.timestamp + upperBound
      *==>80-90=-10 到80+0=80
      */
    val rsDS: DataStream[String] = keyedStream1.intervalJoin(keyedStream2).between(Time.seconds(-90), Time.seconds(0)).process(new JoinStream)
    rsDS.print("rs:")
    env.execute()
  }
}

class JoinStream extends ProcessJoinFunction[(String, Long, String), (String, Long, String), String] {
  override def processElement(left: (String, Long, String), right: (String, Long, String), ctx: ProcessJoinFunction[(String, Long, String), (String, Long, String), String]#Context, out: Collector[String]): Unit = {
    out.collect(left + "=>" + right)
  }
}

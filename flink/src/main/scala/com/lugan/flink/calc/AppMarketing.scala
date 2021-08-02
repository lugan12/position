package com.lugan.flink.calc

import com.lugan.flink.entity.{MarketingUserBehavior, MarketingViewCount}
import com.lugan.flink.market.SimulatedEventSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**本例子要点：
  * 1.按照指定的key--key用元组，改变了以前按key分组时使用拼接字符串的格式：Key=a:b:c:d
  * 2.对于延迟数据的处理：
  * 1).直接抛弃迟到的元素;
  * 2).将迟到的元素发送到另一条流中去;
  * 3).可以更新窗口已经计算完的结果，并发出计算结果。
  * 本例子将迟到数据通过侧边流将数据发送到另一条流处理。
  * 3.最常用的数据有kafka，本文通过自定义数据源，来获取数据==>env.addSource(new RichSourceFunction)
  */
object AppMarketing {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val sourceDataStream: DataStream[MarketingUserBehavior] = env.addSource(new SimulatedEventSource)
    val resultDataStream: DataStream[MarketingViewCount] = sourceDataStream.assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior != "UNINSTALL")
      .map(data => ((data.channel, data.behavior), 1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(10))
      //处理迟到数据，放到侧边流中
      .sideOutputLateData(new OutputTag[((String, String), Long)]("late-date"))
      .process(new MarketingCountByChannel)
    resultDataStream.print("rs:")
    //获取迟到侧边流的数据
    resultDataStream.getSideOutput(new OutputTag[((String, String), Long)]("late-date")).print("lateData:")
    env.execute()
  }
}

class MarketingCountByChannel extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow] {
  override def process(key: (String, String), ctx: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
    val channel: String = key._1
    val behavior: String = key._2
    val endTime: String = ctx.window.getEnd.toString
    val startTime: String = ctx.window.getStart.toString
    val size: Int = elements.size
    out.collect(MarketingViewCount(startTime, endTime, channel, behavior, size))
  }
}

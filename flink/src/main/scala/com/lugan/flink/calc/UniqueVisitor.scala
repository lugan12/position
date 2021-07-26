package com.lugan.flink.calc


import com.lugan.flink.entity.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 非布隆过滤器版本的UV统计
  */
object UniqueVisitor {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val sourceDataStream: DataStream[String] = env.readTextFile("D:\\workspace\\position\\flink\\src\\main\\resources\\UserBehavior.csv")
    val usrBehaviorDataStream: DataStream[UserBehavior] = sourceDataStream.map(t => {
      val str: Array[String] = t.split(",")
      UserBehavior(str(0).trim.toLong, str(1).trim.toLong, str(2).trim.toInt, str(3).trim, str(4).trim.toLong)
    })
    val countedDataDstream: DataStream[UvCount] = usrBehaviorDataStream
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountByWindow)
    countedDataDstream.print("count:")
    env.execute()
  }


}

class UvCountByWindow extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    var pvs: Set[Long] = Set[Long]()
    for (in <- input) {
      pvs += in.userId
    }
    out.collect(UvCount(window.getEnd, pvs.size))
  }
}

case class UvCount(windowEnd: Long, uvCount: Long)

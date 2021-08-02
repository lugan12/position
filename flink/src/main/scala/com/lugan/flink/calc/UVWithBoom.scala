package com.lugan.flink.calc

import java.lang

import com.lugan.flink.entity.UserBehavior
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * 布隆过滤器的使用
  * ProcessWindowFunction的使用
  */
object UVWithBoom {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val sourceDataStream: DataStream[String] = env.readTextFile("D:\\workspace\\position\\flink\\src\\main\\resources\\UserBehavior.csv")
    val resultDataStream: DataStream[UvCount] = sourceDataStream.map(t => {
      val str: Array[String] = t.split(",")
      UserBehavior(str(0).trim.toLong, str(1).trim.toLong, str(2).trim.toInt, str(3).trim, str(4).trim.toLong)
    })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.timestamp * 1000)
      .map(data => ("dummykey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger)
      .process(new UvCountWithBloom)
    resultDataStream.print("result:")

    env.execute()
  }
}

// 定义一个布隆过滤器
class Bloom(size: Long) extends Serializable {
  // 位图的总大小，默认16M
  private val cap = if (size > 0) size else 1 << 27

  // 定义hash函数
  def hash(value: String, seed: Int): Long = {
    var result = 0L
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    result & (cap - 1)
  }
}


class UvCountWithBloom extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
  // 定义redis连接
//  lazy val jedis = new Jedis("192.168.204.1", 6379)
  lazy val jedis = new Jedis("localhost", 6379)
  lazy val bloom = new Bloom(1 << 29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    //位图的存贮的key为window.getEnd, value为bitmap
    val bloom = new Bloom(1 << 29)
    //key
    val storiedKey: String = context.window.getEnd.toString
    //value
    val userId: String = elements.last._2.toString
    //获取布隆过滤器值
    val offSet: Long = bloom.hash(userId, 61)
    var count = 0L
    val countStr: String = jedis.hget("count", storiedKey)
    if (countStr != null) {
      count += countStr.toLong
    }
    //匹配redis中是否存在该userId
    val isExist: lang.Boolean = jedis.getbit(storiedKey, offSet)
    //不存在
    if (!isExist) {
      jedis.setbit(userId, offSet, true)
      jedis.hset("count", storiedKey, (count + 1).toString)
      out.collect(UvCount(storiedKey.toLong, count + 1))
    }
    out.collect(UvCount(storiedKey.toLong, count))
  }
}

class MyTrigger extends Trigger[(String, Long), TimeWindow] {
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //每来一条数据就触发一次窗口操作，并清空所有窗口状态
    TriggerResult.FIRE_AND_PURGE
  }
}

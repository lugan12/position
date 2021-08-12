package com.lugan.flink.calc

import java.lang
import java.util.stream.{Collectors, StreamSupport}

import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction}
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 触发器trigger
  */
object EventTimeIntervalTriggerExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val data = env.socketTextStream("192.168.25.151", 7777)
    data.print("data:")
    val mapedDS: DataStream[Device] = data.map(new DeviceTransformer)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Device](Time.seconds(10)) {
        override def extractTimestamp(element: Device): Long = element.ts
      })
    mapedDS.print("maped:")
    val rsDS: DataStream[Device] = mapedDS
      .keyBy(_.id)
      .timeWindow(Time.seconds(3))
      //触发窗口
      .trigger(new EventTimeIntervalTrigger(2000))
      .maxBy(1)
    rsDS.print("rs:")

    env.execute()
    //定义聚合器
    //        .process(new AggregatePrinter)
  }
}

class AggregatePrinter extends ProcessWindowFunction[Device, String, Tuple, TimeWindow]{
  override def process(key: Tuple, context: ProcessWindowFunction[Device, String, Tuple, TimeWindow]#Context, elements: lang.Iterable[Device], out: Collector[String]): Unit = {
//    StreamSupport.stream(elements.spliterator(),false).collect(Collectors.toList)
//    StreamSupport.stream(elements.spliterator(),false).collect(Collectors.toList)
  }
}

/**
  * 事件时间间隔触发器
  * copy from {@link org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger}
  * 重写 {@link org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger#onEventTime(long, TimeWindow, TriggerContext)}
  * 和 {@link org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger#onProcessingTime(long, TimeWindow, TriggerContext)}
  * 方法：
  * 1. 在onElement方法里增加基于处理时间的定时器
  * 2. 在onProcessingTime方法里增加定时器触发后将窗口发出的逻辑
  */
class EventTimeIntervalTrigger(ts: Long) extends Trigger[Device, TimeWindow] {
  private val reduceState: ReducingStateDescriptor[Long] = new ReducingStateDescriptor[Long]("EventTimeIntervalTrigger", new Min, classOf[Long])


  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //    if(time ==window.maxTimestamp()){TriggerResult.FIRE}else{TriggerResult.CONTINUE}
    //三元表达式版本
    val tim = if (time == window.maxTimestamp()) TriggerResult.FIRE else TriggerResult.CONTINUE
    tim
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    ctx.getPartitionedState(reduceState).add(Long.MaxValue)
    TriggerResult.FIRE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    val fireTimeStamp: ReducingState[Long] = ctx.getPartitionedState(reduceState)
    // 清理事件时间定时器
    ctx.deleteEventTimeTimer(window.maxTimestamp())
    // 清理处理时间定时器
    if (fireTimeStamp.get() > 0) {
      ctx.deleteProcessingTimeTimer(fireTimeStamp.get())
    }
    fireTimeStamp.clear()
  }

  override def canMerge: Boolean = {
    true
  }

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    val maxTS: Long = window.maxTimestamp()
    if (maxTS > ctx.getCurrentWatermark) {
      ctx.registerEventTimeTimer(maxTS)
    }
  }

  override def onElement(element: Device, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if (window.maxTimestamp() <= ctx.getCurrentWatermark) {
      TriggerResult.FIRE
    } else {
      ctx.registerEventTimeTimer(window.maxTimestamp())
      val reduce: ReducingState[Long] = ctx.getPartitionedState(reduceState)
      if (reduce.get() < 0 || reduce.get() == null) {
        // 注册一个基于处理时间的计时器
        val wm: Long = ctx.getCurrentWatermark
        val nextFireTimestamp: Long = wm - (wm % ts) + ts
        ctx.registerEventTimeTimer(nextFireTimestamp)
        reduce.add(nextFireTimestamp)
      }
      TriggerResult.CONTINUE
    }
  }

  class Min extends ReduceFunction[Long] {
    override def reduce(value1: Long, value2: Long): Long = {
      Math.min(value1, value2)
    }
  }

}

/**
  * 如果map的转换逻辑过长，可以通过继承MapFunction实现类将部分代码抽取出来
  */
class DeviceTransformer extends MapFunction[String, Device] {
  override def map(data: String): Device = {
    val strs: Array[String] = data.split(",")
    Device(strs(0), strs(1).toDouble, strs(2).toLong)
  }
}

case class Device(id: String, value: Double, ts: Long)
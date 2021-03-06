package com.lugan.flink.market

import java.util.concurrent.TimeUnit
import java.util.{Random, UUID}

import com.lugan.flink.entity.MarketingUserBehavior
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}


/**
  * 通过RichSourceFunction函数这种方式获取数据源
  */
class SimulatedEventSource extends RichSourceFunction[MarketingUserBehavior] {
  // 定义是否运行的标识位
  var running = true
  // 定义用户行为的集合
  val behaviorTypes: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  // 定义渠道的集合
  val channelSets: Seq[String] = Seq("wechat", "weibo", "appstore", "huaweistore")
  // 定义一个随机数发生器
  val rand: Random = new Random()


  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    // 定义一个生成数据的上限
    val maxElements = Long.MaxValue
    var count = 0L

    // 随机生成所有数据
    while (running && count < maxElements) {
      val id = UUID.randomUUID().toString
      val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSets(rand.nextInt(channelSets.size))
      val ts: Long = System.currentTimeMillis()

      if (count % 4 == 0) {
        //每条发一条迟到数据
        ctx.collect(MarketingUserBehavior(id, behavior, channel, (ts - 60 * 1000)))
      } else {
        //发送不迟到数据
        ctx.collect(MarketingUserBehavior(id, behavior, channel, ts))
      }

      count += 1
      TimeUnit.MILLISECONDS.sleep(10L)
    }
  }
}

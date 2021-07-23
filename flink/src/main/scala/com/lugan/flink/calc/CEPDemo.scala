package com.lugan.flink.calc

import java.util

import com.lugan.flink.entity.{OrderEvent, OrderResult}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * CEP监控
  */
object CEPDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val sourceData: DataStream[String] = env.readTextFile("D:\\workspace\\position\\flink\\src\\main\\resources\\OrderLog.csv")
    val orderEventStream = sourceData.map { t =>
      val str: Array[String] = t.split(",")
      OrderEvent(str(0).toLong, str(1), str(2), str(3).toLong)
    }.assignAscendingTimestamps(_.eventTime * 1000)

    //定义一个带时间匹配的时间窗口
    val orderPayPattern = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .followedBy("follow")
      .where(_.eventType == "pay")
      //定义多长的时间过时
      .within(Time.minutes(15))

    //定义输出标签
    val orderTimeOut: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeOut")
    val patternStream = CEP.pattern(orderEventStream.keyBy("orderId"), orderPayPattern)
    val resultStream = patternStream.select(orderTimeOut, new OrderTimeoutSelect(), new OrderPaySelect())
    resultStream.print("rs:")
    resultStream.getSideOutput(orderTimeOut).print("splitStream:")
    env.execute()
  }
}

// 自定义超时事件序列处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderId = map.get("begin").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout")
  }
}

// 自定义正常支付事件序列处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = map.get("follow").iterator().next().orderId
    OrderResult(payedOrderId, "payed successfully")
  }
}

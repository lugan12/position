package com.lugan.flink.calc

import java.math.BigDecimal
import java.{lang, util}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lugan.flink.entity.DCCacheEntity
import com.lugan.flink.util.{ComUtil, MyKafkaUtil}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 分组条件：日期（date）,时间（time）,币种（ccy）,机构，出入
  * 将每条数据的是否为进或者出，先按进或者出两个方向相加，再把进出相加
  * 算子：
  * map（数据结构转换），
  * reduceByKey（按照key进行累加聚合），
  * groupByKey（将多个字段组合成的key的数据归类到同一个key下面），
  * filter（过滤元组中key为空的数据）
  */
object DCCalc {
  def main(args: Array[String]): Unit = {
    //获取env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val sourceDataStream: DataStream[String] = env.addSource(MyKafkaUtil.getConsumer("test"))
    val keyedDataStream: DataStream[(String, BigDecimal)] = sourceDataStream.map(t => {
      val jSONObject: JSONObject = JSON.parseObject(t)
      val json: JSONObject = jSONObject.getJSONObject("data")
      val BTSACTNBR: String = json.get("BTSACTNBR").toString
      val BTSCCYNBR: String = json.get("BTSCCYNBR").toString
      val BTSBRNGLG: String = json.get("BTSBRNGLG").toString
      val BTSREGTIM: String = json.get("BTSREGTIM").toString
      val BTSTRSAMT: String = json.get("BTSTRSAMT").toString
      val BTSONLBAL: String = json.get("BTSONLBAL").toString
      val BTSTXTC2G: String = json.get("BTSTXTC2G").toString
      val BTSTRSDIR: String = json.get("BTSTRSDIR").toString
      val BTSRCDVER: String = json.get("BTSRCDVER").toString
      //只取BTSTXTC2G为N7CP，ONN3或N7PR的数据
      var amt: BigDecimal = BigDecimal.ZERO
      //获取日期
      val dateStr: String = ComUtil.regDate(BTSREGTIM)
      //获取时间
      val timeStr: String = ComUtil.regTime(BTSREGTIM)
      var key: String = "";
      BTSTXTC2G match {
        case "N7CP" | "ONN3" => {
          //取负数
          amt = new BigDecimal(BTSTRSAMT).abs().multiply(new BigDecimal(-1))
          key = BTSACTNBR + ";" + BTSCCYNBR + ";" + BTSBRNGLG + ";" + dateStr + ";" + timeStr + "_OUT"
        }
        case "N7PR" => {
          //取正数
          amt = new BigDecimal(BTSTRSAMT).abs()
          key = BTSACTNBR + ";" + BTSCCYNBR + ";" + BTSBRNGLG + ";" + dateStr + ";" + timeStr + "_IN"
        }
        case _ =>
      }
      (key, amt)
    })
    keyedDataStream.print("keyedData: ")
    val value: DataStream[DCCacheEntity] = keyedDataStream
      .filter(_._1 != "")
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      //      .reduce((x, y) => (x._1, x._2.add(y._2)))
      .aggregate(new DCAgg)
      .keyBy(_.key)
      .process(new countAmt)
    value.print("result: ")
    env.execute()
  }
}


class countAmt extends KeyedProcessFunction[String, DCCacheEntity, DCCacheEntity] {

  private var entityMapState: MapState[String, DCCacheEntity] = _

  override def open(parametears: Configuration): Unit = {
    entityMapState = getRuntimeContext.getMapState(new MapStateDescriptor[String, DCCacheEntity]("entityMap", classOf[String], classOf[DCCacheEntity]))
  }

  override def processElement(value: DCCacheEntity, ctx: KeyedProcessFunction[String, DCCacheEntity, DCCacheEntity]#Context, out: Collector[DCCacheEntity]): Unit = {

    if (entityMapState.contains(value.key)) {
      val entity: DCCacheEntity = entityMapState.get(value.key)
      entity.inBal = entity.inBal.add(value.inBal)
      entity.outBal = value.outBal.add(entity.outBal)
      entityMapState.put(value.key, entity)
    } else {
      entityMapState.put(value.key, value)
    }

    val entities: lang.Iterable[DCCacheEntity] = entityMapState.values()
    val unit: util.Iterator[DCCacheEntity] = entities.iterator()
    while (unit.hasNext) {
      val entity: DCCacheEntity = unit.next()
      //求总和
      entity.sumBal = entity.inBal.add(entity.outBal)
      out.collect(entity)
    }
  }
}

class DCAgg extends AggregateFunction[(String, BigDecimal), (String, BigDecimal), DCCacheEntity] {

  override def add(in: (String, BigDecimal), acc: (String, BigDecimal)): (String, BigDecimal) = (in._1, in._2.add(acc._2))

  override def createAccumulator(): (String, BigDecimal) = ("", BigDecimal.ZERO)

  override def getResult(acc: (String, BigDecimal)): DCCacheEntity = {
    val keys: Array[String] = acc._1.split("_")
    DCCacheEntity(keys(0), keys(1), if (keys(1).equals("IN")) acc._2 else BigDecimal.ZERO, if (keys(1).equals("OUT")) acc._2 else BigDecimal.ZERO, BigDecimal.ZERO)
  }

  override def merge(acc: (String, BigDecimal), acc1: (String, BigDecimal)): (String, BigDecimal) = (acc._1, acc._2.add(acc1._2))
}

class WindowsResult extends WindowFunction[DCCacheEntity, DCCacheEntity, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[DCCacheEntity], out: Collector[DCCacheEntity]): Unit = {
    out.collect(input.iterator.next())
  }
}
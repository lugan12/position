package com.lugan.flink.calc

import java.lang
import java.math.BigDecimal

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lugan.flink.util.{ComUtil, MyKafkaUtil}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Demo02 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val data: DataStream[String] = env.addSource(MyKafkaUtil.getConsumer("test"))
    val value: DataStream[(String, BigDecimal)] = data.map(t => {
      val jsonAll: JSONObject = JSON.parseObject(t)
      val json: JSONObject = jsonAll.getJSONObject("data")
      val BTSACTNBR: String = json.get("BTSACTNBR").toString
      val BTSCCYNBR: String = json.get("BTSCCYNBR").toString
      val BTSBRNGLG: String = json.get("BTSBRNGLG").toString
      val BTSREGTIM: String = json.get("BTSREGTIM").toString
      val BTSTRSAMT: String = json.get("BTSTRSAMT").toString
      val BTSONLBAL: String = json.get("BTSONLBAL").toString
      val BTSTXTC2G: String = json.get("BTSTXTC2G").toString
      val BTSTRSDIR: String = json.get("BTSTRSDIR").toString
      val BTSRCDVER: String = json.get("BTSRCDVER").toString

      var amt: BigDecimal = BigDecimal.ZERO;
      var bal: BigDecimal = BigDecimal.ZERO;
      var regStr: String = "";

      BTSTXTC2G match {
        //取该指标的相反数
        case "N7CP" | "ONN3" => {
          amt = new BigDecimal(BTSTRSAMT).abs().multiply(new BigDecimal(-1))
          val dateStr: String = ComUtil.regDate(BTSREGTIM)
          val timeStr: String = ComUtil.regTime(BTSREGTIM)
          regStr = dateStr + ";" + timeStr + ";" + BTSCCYNBR + ";" + BTSBRNGLG + "_OUT"
        }
        case "N7PR" => {
          amt = new BigDecimal(BTSTRSAMT).abs()
          val dateStr: String = ComUtil.regDate(BTSREGTIM)
          val timeStr: String = ComUtil.regTime(BTSREGTIM)
          regStr = dateStr + ";" + timeStr + ";" + BTSCCYNBR + ";" + BTSBRNGLG + "_IN"
        }
        case _ => ""
      }
      (regStr, amt)
    })
    value.print("print(1):")
    val reducedDS: DataStream[(String, BigDecimal)] = value
      .keyBy(_._1)
      .timeWindow(Time.seconds(2))
      //  .aggregate(new Agg)
      .reduce((a, b) => (a._1, a._2.add(b._2)))
    reducedDS.print("result : ")


    env.execute()
  }

  //  class Agg extends AggregateFunction[(String, BigDecimal), BigDecimal, BigDecimal] {
  //
  //    override def add(in: (String, BigDecimal), acc: BigDecimal): BigDecimal = {
  //      in._2.add(acc)
  //    }
  //
  //    override def createAccumulator(): BigDecimal = BigDecimal.ZERO
  //
  //    override def getResult(acc: BigDecimal):  BigDecimal = {
  //      acc
  //    }
  //
  //    override def merge(acc: BigDecimal, acc1: BigDecimal): BigDecimal = {
  //      acc.add(acc1)
  //    }
  class Agg extends AggregateFunction[(String, BigDecimal), (String, BigDecimal), (String, BigDecimal)] {
    private var key: String = _;

    override def add(in: (String, BigDecimal), acc: (String, BigDecimal)): (String, BigDecimal) = {
      key = in._1
      (in._1, in._2.add(acc._2))
    }

    override def createAccumulator(): (String, BigDecimal) = {
      (key,BigDecimal.ZERO)
    }

    override def getResult(acc: (String, BigDecimal)): (String, BigDecimal) = {
      (acc._1, acc._2)
    }

    override def merge(in: (String, BigDecimal), out: (String, BigDecimal)): (String, BigDecimal) = {
      (in._1, out._2.add(in._2))
    }
  }

  class WindowsResult() extends WindowFunction[BigDecimal, (BigDecimal, Long, BigDecimal), BigDecimal, TimeWindow] {
    override def apply(key: BigDecimal, window: TimeWindow, input: lang.Iterable[BigDecimal], out: Collector[(BigDecimal, Long, BigDecimal)]): Unit = {
      out.collect((key, window.getEnd, input.iterator().next()))
    }

  }

}

package com.lugan.flink.calc

import java.math.BigDecimal

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lugan.flink.entity.WBActSumBalJson
import com.lugan.flink.util.MyKafkaUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * 账户实时余额走势
  * 侧边流--多流合并和同一kafka topic数据分流
  * 两个kafka数据流，根据业务需要将两个流合并成一个数据流
  */
object WBActSumTim {
  def main(args: Array[String]): Unit = {
    //获取env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val sourceData: DataStream[String] = env.addSource(MyKafkaUtil.getConsumer("act"))
    val mapedDataStream: DataStream[WBActSumBalJson] = sourceData.map { t => {
      val jSONObject: JSONObject = JSON.parseObject(t)
      val json: JSONObject = jSONObject.getJSONObject("data")
      val typ: String = json.getString("type")
      val actNbr: String = json.getString("ACTNBR")
      val ccy: String = json.getString("CCYNBR")
      val tim: String = json.getString("REGTIM")
      val amtStr: String = json.getString("TRSAMT")
      val dir: String = json.getString("TRSDIR")
      val cod: String = json.getString("PRDCOD")
      val sts: String = json.getString("PRCSTS")
      val rcv: String = json.getString("RCVEAC")
      val amt = new BigDecimal(amtStr)
      WBActSumBalJson(typ, actNbr, ccy, tim.toLong, amt, dir, cod, sts,rcv)
    }
    }

    val splitDataStream: DataStream[WBActSumBalJson] = mapedDataStream.process(new SplitStream)
    mapedDataStream.print("total:")
    val kppDataStream: DataStream[WBActSumBalJson] = splitDataStream.getSideOutput(new OutputTag[WBActSumBalJson]("kpp"))
    kppDataStream.print("kpp:")
    val kppMapedDataStream: DataStream[WBActSumBalJson] = kppDataStream.map { t =>
      //kpp的过滤条件：dir=D,sts=22,cod=95100
      //多个条件匹配使用元组
      var act: WBActSumBalJson = null;
        (t.dir, t.sts, t.cod) match {
        case ("D", "22", "95100") => {
          act = getWBActSumBalJson(t.typ, t.actNbr, t.ccyNbr, t.regTim, t.amt, t.dir, t.cod, t.sts,t.rcv)
        }
        case _ => act = getWBActSumBalJson("del", t.actNbr, t.ccyNbr, t.regTim, t.amt, t.dir, t.cod, t.sts,t.rcv)
      }
      act
    }.filter(_.typ == "del")
    kppMapedDataStream.print("kpp:")

    val kqpDataStream: DataStream[WBActSumBalJson] = splitDataStream.getSideOutput(new OutputTag[WBActSumBalJson]("kqp"))
    kqpDataStream.print("kqp:")
    val kqpMapedDataStream: DataStream[WBActSumBalJson] = kqpDataStream.map { t =>
      var act :WBActSumBalJson = null;
        (t.dir, t.cod, t.sts) match {
        case ("C", "95100", "20") => {
          act = getWBActSumBalJson(t.typ, t.actNbr, t.ccyNbr, t.regTim, t.amt, t.dir, t.cod, t.sts,t.rcv)
        }
        case _ =>act = getWBActSumBalJson("del", t.actNbr, t.ccyNbr, t.regTim, t.amt, t.dir, t.cod, t.sts,t.rcv)
      }
      act
    }.filter(_.typ == "del")
    kqpMapedDataStream.print("kqped:")

    /**
      * 流的合并
      */
    val mergedDataStream: DataStream[WBActSumBalJson] = kqpMapedDataStream.union(kppMapedDataStream)
    mergedDataStream.print("merged:")
    env.execute()
  }

  /**
    * 组装对象
    * @param typ
    * @param actNbr
    * @param ccyNbr
    * @param regTim
    * @param amt
    * @param dir
    * @param cod
    * @param sts
    * @return
    */
  def getWBActSumBalJson(typ:String,actNbr:String,ccyNbr:String,regTim:Long,amt:BigDecimal,dir:String,cod:String,sts:String,rcv:String):WBActSumBalJson={
    WBActSumBalJson(typ,actNbr,ccyNbr,regTim,amt,dir,cod,sts,rcv)
  }
}

/**
  * 切分流
  */
class SplitStream extends ProcessFunction[WBActSumBalJson, WBActSumBalJson] {
  lazy val KPP = new OutputTag[WBActSumBalJson]("kpp")
  lazy val KQP = new OutputTag[WBActSumBalJson]("kqp")

  override def processElement(value: WBActSumBalJson, ctx: ProcessFunction[WBActSumBalJson, WBActSumBalJson]#Context, out: Collector[WBActSumBalJson]): Unit = {
    value.typ match {
      case "kpp" => {
        ctx.output(KPP, value)
      }
      case "kqp" => {
        ctx.output(KQP, value)
      }
    }
  }
}

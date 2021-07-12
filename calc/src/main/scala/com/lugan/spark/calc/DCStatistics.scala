package com.lugan.spark.calc

import java.math.BigDecimal
import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lugan.Constant
import com.lugan.spark.entity.DCEntity
import com.lugan.util.{HbaseUtil, MyKafkaUtil, SparkUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 分组条件：日期（date）,时间（time）,币种（ccy）,机构，出入
  * 将每条数据的是否为进或者出，先按进或者出两个方向相加，再把进出相加
  * 算子：
  * map（数据结构转换），
  * reduceByKey（按照key进行累加聚合），
  * groupByKey（将多个字段组合成的key的数据归类到同一个key下面），
  * filter（过滤元组中key为空的数据）
  */
object DCStatistics {
  var family:String = "digitCur";
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("DCStatistics")
    val context = new StreamingContext(conf, Seconds(5))
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Constant.KAFKA_TOPIC, context)

    val trxDStream: DStream[(String, BigDecimal)] = kafkaStream.map(t => {
      val str: String = t.value()
      val json: JSONObject = JSON.parseObject(str)
      val data: String = json.get("data").toString
      val dataJson: JSONObject = JSON.parseObject(data)
      val BTSACTNBR: String = dataJson.get("BTSACTNBR").toString
      val BTSCCYNBR: String = dataJson.get("BTSCCYNBR").toString
      val BTSBRNGLG: String = dataJson.get("BTSBRNGLG").toString
      val BTSREGTIM: String = dataJson.get("BTSREGTIM").toString
      val BTSTRSAMT: String = dataJson.get("BTSTRSAMT").toString
      val BTSONLBAL: String = dataJson.get("BTSONLBAL").toString
      val BTSTXTC2G: String = dataJson.get("BTSTXTC2G").toString
      val BTSTRSDIR: String = dataJson.get("BTSTRSDIR").toString
      val BTSRCDVER: String = dataJson.get("BTSRCDVER").toString

      var amt: BigDecimal = BigDecimal.ZERO;
      var bal: BigDecimal = BigDecimal.ZERO;
      var regStr: String = "";

      BTSTXTC2G match {
        case "N7CP" | "ONN3" => {
          amt = new BigDecimal(BTSTRSAMT).abs().multiply(new BigDecimal(-1))
          //日期转换
          val dateStr: String = SparkUtil.regDate(BTSREGTIM)
          val timeStr: String = SparkUtil.regTime(BTSREGTIM)
          regStr = dateStr + ";" + timeStr + ";" + BTSCCYNBR + ";" + BTSBRNGLG + "_OUT"
        }
        case "N7PR" => {
          amt = new BigDecimal(BTSTRSAMT).abs()
          //日期转换
          val dateStr: String = SparkUtil.regDate(BTSREGTIM)
          val timeStr: String = SparkUtil.regTime(BTSREGTIM)
          regStr = dateStr + ";" + timeStr + ";" + BTSCCYNBR + ";" + BTSBRNGLG + "_IN"
        }
        case _ =>
      }
      (regStr, amt)
    }
      //过滤key为空（不为N7CP ONN3 N7PR）的数据
    ).filter { x => x._1 != "" }

    //数据先按出（N7CP，ONN3），回（N7PR）归类合并
    val trxSumDstream: DStream[(String, BigDecimal)] = trxDStream.reduceByKey((x, y) => x.add(y))

    val mapedTrxSumDStream: DStream[(String, (String, BigDecimal))] = trxSumDstream.map {
      case (key, sum) => {
        val keys: Array[String] = key.split("_")
        (keys(0), (keys(1), sum))
      }
    }

    val mapTrxSumDStream: DStream[(String, (String, BigDecimal))] = trxSumDstream.map { t =>
      val key: String = t._1
      val keys: Array[String] = key.split("_")
      (keys(0), (keys(1), t._2))
    }
    mapTrxSumDStream

    //先按分组条件分组(k,v)=>(k,(k,v))
    val groupedTrxSumDStream: DStream[(String, Iterable[(String, BigDecimal)])] = mapedTrxSumDStream.groupByKey()

    val mapedDStream: DStream[(String, Map[String, BigDecimal])] = groupedTrxSumDStream.map(t => {
      val key: String = t._1
      val value: Iterable[(String, BigDecimal)] = t._2
      val map: Map[String, BigDecimal] = value.toMap
      (key, map)
    })

    //进和出累加
    val sumDStream: DStream[(String, BigDecimal, BigDecimal, BigDecimal)] = mapedDStream.map(t => {
      val key: String = t._1
      val value: Map[String, BigDecimal] = t._2
      val outAmt: BigDecimal = valNullToZero(value.get("OUT"))
      val inAmt: BigDecimal = valNullToZero(value.get("IN"))
      //相加
      val sumData: BigDecimal = outAmt.add(inAmt)
      (key, outAmt, inAmt, sumData)
    })

    //对象组装
    val dcDStream: DStream[DCEntity] = sumDStream.map(t => {
      val key: String = t._1
      val keys: Array[String] = key.split(";")
      val outAmt: BigDecimal = t._2
      val inAmt: BigDecimal = t._3
      val sumAmt: BigDecimal = t._4
      DCEntity(keys(0), keys(1), keys(2), keys(3), outAmt, inAmt, sumAmt)
    })
    dcDStream.print()

    val value: DStream[Unit] = dcDStream.map(dc => {
      val uuid: String = UUID.randomUUID().toString.replace("-", "")
      HbaseUtil.insert("dc_sum_mon", uuid, family, "date", dc.date)
      HbaseUtil.insert("dc_sum_mon", uuid, family, "time", dc.time)
      HbaseUtil.insert("dc_sum_mon", uuid, family, "ccyCod", dc.ccyCod)
      HbaseUtil.insert("dc_sum_mon", uuid, family, "brn", dc.brn)
      HbaseUtil.insert("dc_sum_mon", uuid, family, "outAmt", dc.outAmt.toString())
      HbaseUtil.insert("dc_sum_mon", uuid, family, "inAmt", dc.inAmt.toString())
      HbaseUtil.insert("dc_sum_mon", uuid, family, "sumAmt", dc.sumAmt.toString())
    })
    value.print()

    context.start()
    context.awaitTermination()
  }

  //空转0
  def valNullToZero(x: Option[BigDecimal]) = x match {
    case Some(s) => s;
    case None => BigDecimal.ZERO
  }

}

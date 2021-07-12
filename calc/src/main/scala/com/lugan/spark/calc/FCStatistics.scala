package com.lugan.spark.calc

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lugan.Constant
import com.lugan.spark.entity.FCEntity
import com.lugan.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import java.math.BigDecimal
import java.util.UUID

object FCStatistics {
  def main(args: Array[String]): Unit = {
    //创建连接
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FCStatistics")
    val sc = new StreamingContext(conf, Seconds(5))
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Constant.TOPIC_FC, sc)

    //redis取过滤条件
//    val jedis: Jedis = RedisUtil.getJedisClient
//    val actSet: util.Set[String] = jedis.smembers("innerAct")
//    print(" actNbrs is: "+actSet)
//    // 广播变量,用来序列化数据
//    val broadcastSet: Broadcast[util.Set[String]] = sc.sparkContext.broadcast(actSet)
//    jedis.close()

    val FCMessageDStream: DStream[FCEntity] = kafkaDStream.map { t =>
      val value: String = t.value()
      val str: JSONObject = JSON.parseObject(value)
      val json: JSONObject = str.getJSONObject("data")
      val actNbr: String = json.getString("ACT_NBR")
      val ccyCod: String = json.getString("CCY_NBR")
      val amt: String = json.getString("TRX_AMT")
      val dir: String = json.getString("BOK_DIR")

      FCEntity(actNbr, ccyCod, amt, dir)
    }

//    //过滤指定账户
//    val filterdDStream: DStream[FCEntity] = FCMessageDStream.filter(mssage => {
//      val key = mssage.actNbr + ":" + mssage.ccyCod
//      !actSet.contains(key)
//    })

    //使用transform来过滤，transform算子可以周期性的拉取数据
    val filterdDStream: DStream[FCEntity] = FCMessageDStream.transform(rdd => {
      //redis取过滤条件
      val jedis: Jedis = RedisUtil.getJedisClient
      val actSet: util.Set[String] = jedis.smembers("innerAct")
      // 广播变量,用来序列化数据
      val broadcastSet: Broadcast[util.Set[String]] = sc.sparkContext.broadcast(actSet)
      jedis.close()
      rdd.filter(massege => {
        val key = massege.actNbr + ":" + massege.ccyCod
        !broadcastSet.value.contains(key)
      })
    })
    //组装数据
    val mapedDStream: DStream[(String, BigDecimal)] = filterdDStream.map { t =>
      val key = t.actNbr + ":" + t.ccyCod + ":" + t.dir
      (key, new BigDecimal(t.amt))
    }
    val reducedDStream: DStream[(String, BigDecimal)] = mapedDStream.reduceByKey((x,y)=>x.add(y))

    val fcDStream: DStream[FCEntity] = reducedDStream.map { t =>
      val key: String = t._1
      val keys: Array[String] = key.split(":")
      FCEntity(keys(0), keys(1), t._2.toString, keys(2))
    }
    fcDStream.print()
    fcDStream.map{ t=>
      val uuid: String = UUID.randomUUID().toString.replace("-", "")

    }

    sc.start()
    sc.awaitTermination()
  }
}

package com.lugan.flink.calc
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{PurgingTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, Window}

/**
  * 触发器：先用reduce算子将数据累加，当每次指定新进来的数据累加计算后超过80，就会触发Trigger算子进行计算
  */
object TriggerDemo {
  def main(args: Array[String]): Unit = {

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val data = env.socketTextStream("192.168.25.151", 7777)

      // data is obtained in "name,mark" format
      val fdata = data.map { values =>
        val columns = values.split(",")
        (columns(0), columns(1).toDouble, 1)
      }
    fdata.print("fdata:")

      // calculating average mark and number of exam attempts
      val keyed1 = fdata.keyBy(0).reduce { (x,y) =>
        (x._1, x._2 + y._2, x._3 + y._3)
      }
        .map( x => Marks(x._1, x._2 / x._3, x._3))
    keyed1.print("keyed1:")

      val keyed = keyed1.keyBy(_.name)
        .window(GlobalWindows.create())
//          .timeWindow(Time.seconds(10))
        .trigger(PurgingTrigger.of(new MarksTrigger[GlobalWindow]())).
        maxBy(1)

      keyed.print("keyed:")
      env.execute()

    }
}

class MarksTrigger[W <: Window] extends Trigger[Marks,W] {

  override def onElement(element: Marks, timestamp: Long, window: W, ctx: TriggerContext): TriggerResult = {
    //trigger is fired if average marks of a student cross 80
    if(element.mark > 80) TriggerResult.FIRE
    else TriggerResult.CONTINUE
  }

  override def onProcessingTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }
  override def onEventTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: W, ctx: TriggerContext) = ???
}

case class Marks(name : String, mark : Double, count : Int)

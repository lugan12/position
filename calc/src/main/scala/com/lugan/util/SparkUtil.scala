package com.lugan.util

import java.text.SimpleDateFormat
import java.util.Date

object SparkUtil {
  def str2Date(strDate: String): Date = {
    val date: Date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(strDate)
    date
  }

  def regDate(strDate: String): String = {
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date: Date = format.parse(strDate)
    format.format(date)
  }

  def regTime(strDate: String): String = {
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss")
    val formatTime = new SimpleDateFormat("HH:mm")
    val date: Date = format.parse(strDate)
    formatTime.format(date)
  }

  def str2Bigdecimal(strAmt: String): BigDecimal = {
    BigDecimal(strAmt)
  }

  def main(args: Array[String]): Unit = {
   val a = "14"
    val x = a match{
      case "2" => {
        print("ok")
      }
      case "3" =>{
        print("not ok")
      }
      case "4" => {
        print("very ok")
      }
      case _ => {
        print("oo")
      }
    }

  }


}

package com.lugan.flink.calc

object demo01 {
  def main(args: Array[String]): Unit = {
    val tup = ("1","b","c")
    tup match {
      case ("a","b","c") =>{
        print("aa")
      }
      case _ => print("v")
    }
  }
}

package com.lugan.util

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {

  def main(args: Array[String]): Unit = {
    val properties: Properties = PropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }
  def load (propertiesName:String): Properties ={
    val pro = new Properties()
    pro.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),"UTF-8"))
  pro
  }
}

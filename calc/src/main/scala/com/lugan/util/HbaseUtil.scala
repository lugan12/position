package com.lugan.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

object HbaseUtil {

  val conf: Configuration = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("hbase.zookeeper.quorum", "192.168.25.150")
  val connection= ConnectionFactory.createConnection(conf)

  def insert(tab: String, rowkey: String, conlumn: String, key: String, value: String): Unit = {
    val tableName: TableName = TableName.valueOf(tab)
    val table: Table = connection.getTable(tableName)
    val put = new Put(rowkey.getBytes)
    put.addColumn(conlumn.getBytes, key.getBytes, value.getBytes)
    table.put(put)
  }

  def main(args: Array[String]): Unit = {
    val admin: Admin = connection.getAdmin
    insert("student","0039","info","11sex","ee")
  }
}

package com.lugan.flink.entity

case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )

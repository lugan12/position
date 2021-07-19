package com.lugan.flink.entity

import java.math.BigDecimal

case class DCCacheEntity(val key: String, dir: String, var inBal: BigDecimal, var outBal: BigDecimal, var sumBal: BigDecimal)

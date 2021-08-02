package com.lugan.flink.entity

case class MarketingViewCount( windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long )
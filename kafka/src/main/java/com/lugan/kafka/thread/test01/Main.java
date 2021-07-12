package com.lugan.kafka.thread.test01;

import com.lugan.spark.kafka.thread.test01.ConsumerHandler;

public class Main {

    public static void main(String[] args) {
        String brokerList = "192.168.25.150:9092";
        String groupId = "test";
        String topic = "test";
        int workerNum = 5;

        ConsumerHandler consumers = new ConsumerHandler(brokerList, groupId, topic);
        consumers.execute(workerNum);
        try {
            Thread.sleep(1000000);
        } catch (InterruptedException ignored) {}
        consumers.shutdown();
    }
}
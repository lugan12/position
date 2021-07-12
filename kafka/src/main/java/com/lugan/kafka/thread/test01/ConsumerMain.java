package com.lugan.kafka.thread.test01;

public class ConsumerMain {
    public static void main(String[] args) {
        String brokerList = "192.168.25.150:9092";
        String groupId = "test";
        String topic = "test";
        int consumerNum = 3;

        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum, groupId, topic, brokerList);
        consumerGroup.execute();
    }
}

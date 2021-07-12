package com.lugan.kafka.thread.test03;

public class Test {
    public static void main(String[] args) throws InterruptedException {
        int expectedCount = 3;
        String brokerId = "192.168.25.150:9092";
        String groupId = "test";
        String topic = "test";

        OrdinaryConsumer consumer = new OrdinaryConsumer(brokerId, topic, groupId + "-single", expectedCount);
        long start = System.currentTimeMillis();
        consumer.run();
        System.out.println("Single-threaded consumer costs " + (System.currentTimeMillis() - start));

        Thread.sleep(1L);

        MultiThreadedConsumer multiThreadedConsumer =
                new MultiThreadedConsumer(brokerId, topic, groupId + "-multi", expectedCount);
        start = System.currentTimeMillis();
        multiThreadedConsumer.run();
        System.out.println("Multi-threaded consumer costs " + (System.currentTimeMillis() - start));
    }
}

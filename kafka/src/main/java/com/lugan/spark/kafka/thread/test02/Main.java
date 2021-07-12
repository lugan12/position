package com.lugan.spark.kafka.thread.test02;

public class Main {

    public static void main(String[] args) {
        String brokerList = "192.168.25.150:9092";
        String topic = "test";
        String groupID = "test";
        final ConsumerThreadHandler<byte[], byte[]> handler = new ConsumerThreadHandler<>(brokerList, groupID, topic);
        final int cpuCount = Runtime.getRuntime().availableProcessors();

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                handler.consume(cpuCount);
            }
        };
        new Thread(runnable).start();

        try {
            // 20秒后自动停止该测试程序
            Thread.sleep(20000L);
        } catch (InterruptedException e) {
            // swallow this exception
        }
        System.out.println("Starting to close the consumer...");
        handler.close();
    }
}
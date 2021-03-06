package com.lugan.kafka.thread.test03;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class MultiThreadedConsumer {
    private final Map<TopicPartition, ConsumerWorker<String, String>> outstandingWorkers = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    private long lastCommitTime = System.currentTimeMillis();
    private final Consumer<String, String> consumer;
    private final int DEFAULT_COMMIT_INTERVAL = 3000;
    private final Map<TopicPartition, Long> currentConsumedOffsets = new HashMap<>();
    private final long expectedCount;

    private final static Executor executor = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() * 10, r -> {
                Thread t = new Thread(r);
                t.setDaemon(true);
                return t;
            });

    public MultiThreadedConsumer(String brokerId, String topic, String groupID, long expectedCount) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerId);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic), new MultiThreadedRebalanceListener(consumer, outstandingWorkers, offsetsToCommit));
        this.expectedCount = expectedCount;
    }

    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                distributeRecords(records);
                checkOutstandingWorkers();
                commitOffsets();
                if (currentConsumedOffsets.values().stream().mapToLong(Long::longValue).sum() >= expectedCount) {
                    break;
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * ??????????????????????????????????????????????????????resume??????
     */
    private void checkOutstandingWorkers() {
        Set<TopicPartition> completedPartitions = new HashSet<>();
        outstandingWorkers.forEach((tp, worker) -> {
            if (worker.isFinished()) {
                completedPartitions.add(tp);
            }
            long offset = worker.getLatestProcessedOffset();
            currentConsumedOffsets.put(tp, offset);
            if (offset > 0L) {
                offsetsToCommit.put(tp, new OffsetAndMetadata(offset));
            }
        });
        completedPartitions.forEach(outstandingWorkers::remove);
        consumer.resume(completedPartitions);
    }

    /**
     * ????????????
     */
    private void commitOffsets() {
        try {
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastCommitTime > DEFAULT_COMMIT_INTERVAL && !offsetsToCommit.isEmpty()) {
                consumer.commitSync(offsetsToCommit);
                offsetsToCommit.clear();
            }
            lastCommitTime = currentTime;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * ?????????????????????????????????????????????????????????????????????????????????
     * @param records
     */
    private void distributeRecords(ConsumerRecords<String, String> records) {
        if (records.isEmpty())
            return;
        Set<TopicPartition> pausedPartitions = new HashSet<>();
        records.partitions().forEach(tp -> {
            List<ConsumerRecord<String, String>> partitionedRecords = records.records(tp);
            pausedPartitions.add(tp);
            final ConsumerWorker<String, String> worker = new ConsumerWorker<>(partitionedRecords);
            CompletableFuture.supplyAsync(worker::run, executor);
            outstandingWorkers.put(tp, worker);
        });
        consumer.pause(pausedPartitions);
    }

}

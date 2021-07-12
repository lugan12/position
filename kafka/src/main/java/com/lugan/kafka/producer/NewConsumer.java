package com.lugan.kafka.producer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;


public class NewConsumer {

public static void  pollDataFromKafka(){
        Properties props = new Properties();
    props.put("bootstrap.servers", "192.168.25.150:9092,192.168.25.151:9092");
    props.put("group.id", "test");//消费者组，只要group.id相同，就属于同一个消费者组
    props.put("enable.auto.commit", "false");//自动提交offset
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("max.poll.records","15");
        //1.创建1个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("sourceData"));

        //2.调用poll
while (true){
    ArrayList<ConsumerRecord<String, String>> kafkaData = new ArrayList<>();
    ConsumerRecords<String, String> records = consumer.poll(100);
    for (ConsumerRecord<String, String> record : records) {
        kafkaData.add(record);
                System.out.println("topic = " + record.topic() + " offset = " + record.offset() + " value = " + record.value());
            }
            consumer.commitAsync();
//            consumer.commitSync();

//        return records;
    }
}

    public static void main(String[] args) {
        pollDataFromKafka();
    }

    }



package com.lugan.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class ActProducer {
    public static void sendToKafka(String s) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.25.150:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //1.创建1个生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        //storm版本的聚合
        String TOPIC = "act";
        producer.send(new ProducerRecord<String, String>(TOPIC, s));

        //3.关闭生产者
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i < 4; i++) {
            Thread.sleep(100);
            sendToKafka(actStr());
        }
    }

    private static String actStr() {
        long ts = System.currentTimeMillis();
        String[] type = {"kpp", "kqp"};
        int i1 = (int) (Math.random() * 2);
        String[] dir = {"C", "D"};
        String[] cod = {"95100", "95500", "99800"};
        int i3 = (int) (Math.random() * 3);
        String[] rcv = {"95100033", "95101233", "95101033"};
        int i5 = (int) (Math.random() * 3);
        String[] sts = {"20", "22", "33", "44"};
        int i4 = (int) (Math.random() * 4);
        int i2 = (int) (Math.random() * 2);
        String[] ccys = {"10", "13", "20", "25", "30"};
        int num1 = (int) (Math.random() * 5);
        String[] brns = {"110", "123", "125", "755", "128"};
        int num2 = (int) (Math.random() * 5);
        int max = 99999999;
        int min = 9999999;

        Random random = new Random();
        int num4 = random.nextInt(max) % (max + min + 1) + min;

        String s = "{\"send_time\":" + ts + ",\"data\":{\"type\":\"" + type[i1] + "\",\"ACTNBR\":\"95132423559823\",\"CCYNBR\":\"" + ccys[num1] + "\",\"BRNGLG\":\"" + brns[num2] + "\",\"REGTIM\":\""+ts+"\",\"TRSAMT\":\"" + String.valueOf(num4) + "\",\"TRSDIR\":\"" + dir[i2] + "\",\"PRDCOD\":\"" + cod[i3] + "\",\"RCVEAC\":\"" + rcv[i5] + "\",\"PRCSTS\":\"" + sts[i4] + "\"}}";
        System.out.println(s);
        return s;
    }
}

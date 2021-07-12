package com.lugan.kafka.producer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class DCProducer {

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
        String TOPIC = "test";
        producer.send(new ProducerRecord<String, String>(TOPIC, s));

        //3.关闭生产者
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
//        String s1 = "{\"type\":\"DigitCurrency\",\"send_time\":\"2021-06-03 15.28.34\",\"data\":{\"BTSACTNBR\":\"95132423559823\",\"BTSCCYNBR\":\"10\",\"BTSBRNGLG\":\"110\",\"BTSREGTIM\":\"2021-06-03 15.28.34\",\"BTSTRSAMT\":\"20000\",\"BTSONLBAL\":\"49530656\",\"BTSTXTC2G\":\"N7CP\",\"BTSTRSDIR\":\"借方\",\"BTSRCDVER\":\"1\"}}";
//        String s2 = "{\"type\":\"DigitCurrency\",\"send_time\":\"2021-06-03 15.28.34\",\"data\":{\"BTSACTNBR\":\"95132423559823\",\"BTSCCYNBR\":\"10\",\"BTSBRNGLG\":\"110\",\"BTSREGTIM\":\"2021-06-03 15.28.34\",\"BTSTRSAMT\":\"40000\",\"BTSONLBAL\":\"49530656\",\"BTSTXTC2G\":\"N7PR\",\"BTSTRSDIR\":\"借方\",\"BTSRCDVER\":\"1\"}}";
////        String s3 = "{\"type\":\"DigitCurrency\",\"send_time\":\"2021-06-03 15.28.34\",\"data\":{\"BTSACTNBR\":\"95132423559823\",\"BTSCCYNBR\":\"10\",\"BTSBRNGLG\":\"110\",\"BTSREGTIM\":\"2021-06-03 15.28.34\",\"BTSTRSAMT\":\"10000\",\"BTSONLBAL\":\"49530656\",\"BTSTXTC2G\":\"VF32\",\"BTSTRSDIR\":\"借方\",\"BTSRCDVER\":\"1\"}}";
//        sendToKafka(s1);
//        sendToKafka(s2);
////        sendToKafka(s3);
        for (int i = 0; i <4 ; i++) {
            Thread.sleep(200);
            String s = dcStr();
            sendToKafka(s);
        }

    }

    private static String dcStr() {
        String[] ccys = {"10", "13", "20", "25", "30"};
        int num1 = (int) (Math.random() * 5);
        String[] brns = {"110", "123", "125", "755", "128"};
        int num2 = (int) (Math.random() * 5);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss");
        String strDate = sdf.format(new Date());
        String[] g2g ={"N7CP","ONN3","N7PR","VF32","RJ53"};
        int num3 = (int) (Math.random() * 5);

        int max = 99999999;
        int min = 9999999;

        Random random = new Random();
        int num4 = random.nextInt(max)%(max+min+1)+min;

        int num5 = (int) (Math.random() * 100000000);
        String s = "{\"type\":\"DigitCurrency\",\"send_time\":\""+strDate+"\",\"data\":{\"BTSACTNBR\":\"95132423559823\",\"BTSCCYNBR\":\"" + ccys[num1] + "\",\"BTSBRNGLG\":\"" + brns[num2] + "\",\"BTSREGTIM\":\""+strDate+"\",\"BTSTRSAMT\":\""+String.valueOf(num4)+"\",\"BTSONLBAL\":\""+String.valueOf(num5)+"\",\"BTSTXTC2G\":\""+g2g[num3]+"\",\"BTSTRSDIR\":\"借方\",\"BTSRCDVER\":\"1\"}}";
        return s;

    }

}

package com.lugan.kafka.producer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.xml.bind.SchemaOutputResolver;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class FCProducer {

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
        //指定topic
        String TOPIC = "fcData";
        producer.send(new ProducerRecord<String, String>(TOPIC, s));

        //3.关闭生产者
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i <4 ; i++) {
            Thread.sleep(200);
            String s = dcStr();
            sendToKafka(s);
        }
    }

    private static String dcStr() {
        String[] busSys = {"CF", "SW", "IS", "KF", "KG","CF","L6","GMS","JH","OK","TG"};
        int busSysNum = (int) (Math.random() * 10);
        String[] reaFlg = {"Y", "N"};
        int reaFlgNum = (int) (Math.random() * 2);
        String[] canFlg = {"Y", "N"};
        int canFlgNum = (int) (Math.random() * 2);
        String[] bokDir = {"+", "-"};
        int bokDirNum = (int) (Math.random() * 2);
        String[] ccys = {"10", "13", "20", "25", "30"};
        int num1 = (int) (Math.random() * 5);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String strDate = sdf.format(new Date());
        String[] rptBrn ={"100523","185501","600502","232432","320432"};
        int rptBrnNum = (int) (Math.random() * 5);

        int max = 99999999;
        int min = 9999999;

        Random random = new Random();
        int num4 = random.nextInt(max)%(max+min+1)+min;

        int num5 = (int) (Math.random() * 100000000);
        String s= "{ \"type\": \"FCData\",  \"send_time\": \""+strDate+"\",  \"data\": { \"ACT_NBR\": \"95132423559823\",  \"ACT_NAM\": \"SWIFT测试\",  \"BUS_NBR\": \"123\",  \"BAC_DAT\": \"2021-06-28 19.34.19\", \"CCY_NBR\": \""+ccys[num1]+"\", \"TRX_AMT\": \""+num5+"\", \"BOK_DIR\": \""+bokDir[bokDirNum]+"\",   \"VAL_DAT\": \""+strDate+"\",  \"BUS_SYS\": \""+busSys[busSysNum]+"\", \"PRE_FLG\": \"N\",  \"REA_FLG\": \""+reaFlg[reaFlgNum]+"\",  \"BUS_NBR\": \"123\", \"RPT_BRN\": \""+rptBrn[rptBrnNum]+"\", \"CAN_FLG\": \""+canFlg[canFlgNum]+"\", \"BUS_SEQ\": \"1761\"}}";
        return s;

    }
}

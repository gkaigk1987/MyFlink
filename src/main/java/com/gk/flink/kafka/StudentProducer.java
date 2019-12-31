package com.gk.flink.kafka;

import com.alibaba.fastjson.JSONObject;
import com.gk.flink.domain.Student;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2019/12/30
 */
public class StudentProducer {

    public static final String BROKER_LIST = "192.168.31.251:9092,192.168.31.252:9092,192.168.31.253:9092";

    public static final String TOPIC_STUDENT = "student";

    public static void produceData() {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",BROKER_LIST);
        prop.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(prop);

        int randomInt = RandomUtils.nextInt(1,100);
        Student student = new Student(randomInt, "name" + randomInt, randomInt,new Date().getTime());
        // 发送数据
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_STUDENT, null, null, JSONObject.toJSONString(student));
        producer.send(record);
        System.out.println("kafka 已发送：" + JSONObject.toJSONString(student));
        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(1000);
            produceData();
        }
    }



}

package com.gk.flink.kafka;

import com.alibaba.fastjson.JSONObject;
import com.gk.flink.domain.Student;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2019/12/31
 */
public class DatasourceFromKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.put("bootstrap.servers", StudentProducer.BROKER_LIST);
        //每个消费者分配独立的组号
        properties.put("group.id", "student_group1");
        //如果value合法，则自动提交偏移量
        properties.put("enable.auto.commit", "true");
        //设置多久一次更新被消费消息的偏移量
        properties.put("auto.commit.interval.ms", "1000");
        //设置会话响应的时间，超过这个时间kafka可以选择放弃消费或者消费下一条消息
        properties.put("session.timeout.ms", "30000");
        //自动重置offset
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<>(StudentProducer.TOPIC_STUDENT, new SimpleStringSchema(), properties))
                .setParallelism(1);
        SingleOutputStreamOperator<Student> studentStream = dataStream.map(st -> JSONObject.parseObject(st, Student.class));
        studentStream = studentStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Student>() {
            @Override
            public long extractAscendingTimestamp(Student element) {
                return element.getTimestamp();
            }
        });
        studentStream.keyBy(Student::getId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .sum("age")
                .print();
//        studentStream.print();
        env.execute("Test kafka Datasource");
    }

}

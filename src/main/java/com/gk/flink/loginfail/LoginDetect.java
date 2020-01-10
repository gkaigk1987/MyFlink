package com.gk.flink.loginfail;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;

/**
 * @Description: 指定时间间隔内登录失败检测，用于预警
 * @Author: GK
 * @Date: 2020/1/10
 */
public class LoginDetect {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        URL resource = LoginDetect.class.getClassLoader().getResource("LoginLog.csv");
        SingleOutputStreamOperator<LoginEvent> dataStream = env.readTextFile(resource.getPath())
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new LoginEvent(Integer.valueOf(split[0].trim()), split[1].trim(), split[2].trim(), Long.valueOf(split[3].trim()));
                    }
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                });
        SingleOutputStreamOperator<LoginWarning> loginWarningStream = dataStream.keyBy(LoginEvent::getUserId)
                .process(new LoginProcessFunction());

        loginWarningStream.print("Login Fail");

        env.execute("Login Fail Detect Job");
    }

}

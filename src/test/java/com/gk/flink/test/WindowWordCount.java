package com.gk.flink.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2019/12/25
 */
public class WindowWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String,Integer>> dataStream = environment
                .socketTextStream("localhost",9000)
                .flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);
        dataStream.print();
        environment.execute("Window Word Count");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String,Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word : s.split(" ")) {
//                collector.collect(new Tuple2<>(word,1));
                collector.collect(Tuple2.of(word,1));
            }
        }
    }

}

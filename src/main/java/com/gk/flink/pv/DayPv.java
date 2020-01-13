package com.gk.flink.pv;

import com.gk.flink.domain.PageView;
import com.gk.flink.domain.PvCount;
import com.gk.flink.util.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2020/1/10
 */
public class DayPv {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();
        evn.setParallelism(1);
        evn.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<PageView> dataStream = evn.addSource(new PvProducer());

        SingleOutputStreamOperator<String> sum = dataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PageView>() {
            @Override
            public long extractAscendingTimestamp(PageView element) {
                return element.getCreateTime();
            }
        }).map(new MapFunction<PageView, PvCount>() {
            @Override
            public PvCount map(PageView value) throws Exception {
                String dateTime = DateUtil.long2String(value.getCreateTime() / 1000, "yyyy-MM-dd HH:mm:ss");
                System.out.println(dateTime);
                return new PvCount("pv", 1, dateTime);
            }
        })
                .keyBy(PvCount::getId)
                .timeWindow(Time.days(1))
                .trigger(ContinuousEventTimeTrigger.of(Time.minutes(1)))    //每隔一分钟onfire
                // 第一种实现，通过ProcessFunction
                .process(new PVProcessWindowFunction());
        // 第二种实现，直接sum
              //.sum("count");
        sum.print("Test");

        evn.execute("Pv Count test");
    }
}

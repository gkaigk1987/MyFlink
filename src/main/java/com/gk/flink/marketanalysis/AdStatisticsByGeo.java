package com.gk.flink.marketanalysis;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2020/1/15
 */
public class AdStatisticsByGeo {

    private static OutputTag<BlackListWarning> blackListWarningOutputTag = new OutputTag<>("blackList");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = AdStatisticsByGeo.class.getClassLoader().getResource("AdClickLog.csv");
        SingleOutputStreamOperator<AdClickEvent> dataStream = env.readTextFile(resource.getPath()).map(data -> {
            String[] split = data.split(",");
            return new AdClickEvent(Long.valueOf(split[0].trim()), Long.valueOf(split[1].trim()), split[2].trim(), split[3].trim(), Long.valueOf(split[4].trim()));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
            @Override
            public long extractAscendingTimestamp(AdClickEvent element) {
                return element.getTimeStamp() * 1000L;
            }
        });

//        dataStream.keyBy(ad -> {
//            return new Tuple2(ad.getUserId(),ad.getAdId());
//        }).process(new FilterBlackListUser())

        //获取每个省份的广告点击量
        SingleOutputStreamOperator<CountByProvince> provinceAgg = dataStream.keyBy(AdClickEvent::getProvince)
                .timeWindow(Time.hours(1), Time.seconds(10))    //每10S计算过去一小时的广告点击
                .aggregate(new CountAgg(), new CountWindowFunction());

        provinceAgg.print("Province Count");

        env.execute("ad statistics job");
    }
}

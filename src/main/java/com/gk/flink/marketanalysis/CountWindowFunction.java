package com.gk.flink.marketanalysis;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2020/1/15
 */
public class CountWindowFunction implements WindowFunction<Long,CountByProvince,String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<CountByProvince> out) throws Exception {
        out.collect(new CountByProvince(new Timestamp(window.getEnd()).toString(),s,input.iterator().next()));
    }
}

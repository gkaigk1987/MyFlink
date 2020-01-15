package com.gk.flink.marketanalysis;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2020/1/15
 */
public class FilterBlackListUser extends KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent> {
    @Override
    public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {

    }
}

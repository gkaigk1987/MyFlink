package com.gk.flink.marketanalysis;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2020/1/15
 */
public class CountAgg implements AggregateFunction<AdClickEvent,Long,Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(AdClickEvent value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}

package com.gk.flink.hot;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Description: COUNT 统计的聚合函数实现，每出现一条记录加一
 * @Author: GK
 * @Date: 2019/12/26
 */
public class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior userBehavior, Long aLong) {
        return aLong + 1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong + acc1;
    }
}

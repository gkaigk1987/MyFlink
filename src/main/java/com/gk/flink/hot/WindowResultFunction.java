package com.gk.flink.hot;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Description: 输出窗口的结果
 * @Author: GK
 * @Date: 2019/12/26
 */
public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple key, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
        // key: 窗口的主键，即 itemId
        // window: 窗口
        // input: 聚合函数的结果，即 viewCount 值
        // out: 输出类型为 ItemViewCount
        Long itemId = ((Tuple1<Long>) key).f0;
        Long count = input.iterator().next();
        out.collect(ItemViewCount.of(itemId, window.getEnd(), count));
    }
}

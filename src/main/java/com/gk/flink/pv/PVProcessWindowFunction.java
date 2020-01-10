package com.gk.flink.pv;

import com.gk.flink.domain.PvCount;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2020/1/10
 */
public class PVProcessWindowFunction extends ProcessWindowFunction<PvCount,String, Tuple, TimeWindow> {
    @Override
    public void process(Tuple tuple, Context context, Iterable<PvCount> elements, Collector<String> out) throws Exception {

    }
}

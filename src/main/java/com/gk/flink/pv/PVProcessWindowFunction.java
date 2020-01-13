package com.gk.flink.pv;

import com.gk.flink.domain.PvCount;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2020/1/10
 */
public class PVProcessWindowFunction extends ProcessWindowFunction<PvCount,String, String, TimeWindow> {
//    private ValueState<Integer> countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("countValue",Integer.class));
    @Override
    public void process(String s, Context context, Iterable<PvCount> elements, Collector<String> out) throws Exception {
        Iterator<PvCount> iterator = elements.iterator();
        int count = 0;
        String lastDateTime = null;
        while (iterator.hasNext()) {
            PvCount next = iterator.next();
            count += next.getCount();
            lastDateTime = next.getDateTime();
        }
        long end = context.window().getEnd();
//        out.collect(new PvCount("test",count,lastDateTime));
        out.collect("111"); //此处可以自定义类型进行输出
    }





}

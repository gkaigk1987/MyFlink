package com.gk.flink.pv;

import com.gk.flink.domain.PageView;
import com.gk.flink.domain.PvCount;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2020/1/10
 */
public class PvProducer implements SourceFunction<PageView> {

    private boolean flag = true;

    private int i = 1;

    @Override
    public void run(SourceContext ctx) throws Exception {
        while (true) {
            ctx.collect(new PageView(i,getTime()));
            Thread.sleep(1000);
            i++;
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }

    private Long getTime() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime localDateTime = now.plusMinutes(10L);
        return localDateTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
    }
}

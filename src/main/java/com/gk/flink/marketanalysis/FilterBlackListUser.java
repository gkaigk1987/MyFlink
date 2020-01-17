package com.gk.flink.marketanalysis;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2020/1/15
 */
public class FilterBlackListUser extends KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent> {

    private OutputTag<BlackListWarning> outputTag;

    //每天每个广告每个人可点击的最大次数
    private Long maxCount;

    // 保存当前用户对指定广告的点击量
    private ValueState<Long> count;

    // 当前超过指定点击量的提醒是否会已发出
    private ValueState<Boolean> isSendFlag;

    // 保存定时器触发的时间戳
    private ValueState<Long> resetTime;

    public FilterBlackListUser() {

    }

    public FilterBlackListUser(Long maxCount, OutputTag<BlackListWarning> tag) {
        this.maxCount = maxCount;
        this.outputTag = tag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
        count = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count-state",Long.class));
        isSendFlag = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isSend-state",Boolean.class));
        resetTime = getRuntimeContext().getState(new ValueStateDescriptor<Long>("reset-state",Long.class));
    }

    @Override
    public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
        Long currentCount = count.value();

        if(currentCount != null && currentCount >= maxCount) {
            //点击次数已超过最大限制数
            if(null == isSendFlag.value() || !isSendFlag.value()) {
                // 未发送提醒
                // 侧输出流
                ctx.output(outputTag,new BlackListWarning(value.getUserId(),value.getAdId(),"用户：" + value.getUserId() + ",点击广告：" + value.getAdId() + ",超过：" + maxCount + "次"));
                isSendFlag.update(true);
            }
            return;
        }
        if(currentCount == null || currentCount == 0) {
            //第一次处理，注册定时器，每天凌晨0点触发
            currentCount = 0L;
            Long nextTimestamp = (ctx.timerService().currentProcessingTime()/(1000*60*60*24) + 1) * (1000*60*60*24);//(ctx.timerService().currentProcessingTime() / 1000 * 60 * 60 * 24 + 1) / (1000 * 60 * 60 * 24);
            resetTime.update(nextTimestamp);
            ctx.timerService().registerProcessingTimeTimer(nextTimestamp);  //注册触发时间
        }
        count.update(currentCount + 1);
        out.collect(value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
        // 定时器触发时清空状态
        if(timestamp == resetTime.value()) {
            isSendFlag.clear();
            resetTime.clear();
            count.clear();
        }

    }
}

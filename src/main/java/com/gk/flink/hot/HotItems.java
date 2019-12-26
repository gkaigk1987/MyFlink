package com.gk.flink.hot;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.net.URL;

/**
 * @Description: 如何计算实时热门商品
 * @Author: GK
 * @Date: 2019/12/25
 *
 * 如何基于 EventTime 处理，如何指定 Watermark
 * 如何使用 Flink 灵活的 Window API
 * 何时需要用到 State，以及如何使用
 * 如何使用 ProcessFunction 实现 TopN 功能
 *
 * 抽取出业务时间戳，告诉 Flink 框架基于业务时间做窗口
 * 过滤出点击行为数据
 * 按一小时的窗口大小，每5分钟统计一次，做滑动窗口聚合（Sliding Window）
 * 按每个窗口聚合，输出每个窗口中点击量前N名的商品
 */
public class HotItems {

    public static void main(String[] args) throws Exception {
        // 创建 execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 按照 EventTime 处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，改变并发对结果正确性没有影响
        env.setParallelism(1);

        URL path = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(path.toURI()));
        // 抽取 UserBehavior 的 TypeInformation，是一个 PojoTypeInfo
        PojoTypeInfo<UserBehavior> pojoTypeInfo = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        // 创建 PojoCsvInputFormat
        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoTypeInfo, fieldOrder);
        env.createInput(csvInput,pojoTypeInfo)  // 创建数据源，得到 UserBehavior 类型的 DataStream
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                // 抽取出时间和生成 watermark
                @Override
                public long extractAscendingTimestamp(UserBehavior element) {
                    // 原始数据单位秒，将其转成毫秒
                    return element.getTimestamp() * 1000;
                }
            })
            .filter(new FilterFunction<UserBehavior>() {    // 过滤出只有点击的数据
                @Override
                public boolean filter(UserBehavior value) throws Exception {
                    return value.getBehavior().equalsIgnoreCase("pv");
                }
            })
            .keyBy("itemId")
            .timeWindow(Time.minutes(60),Time.minutes(5))
            .aggregate(new CountAgg(),new WindowResultFunction())   //聚合
            .keyBy("windowEnd")
            .process(new TopNHotItems(3))
            .print();
        env.execute("Hot Items Job");
    }

}

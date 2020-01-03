package com.gk.flink.source;

import com.gk.flink.domain.Student;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2020/1/3
 */
public class MySourceFunction implements SourceFunction<Student> {

    private boolean flag = true;

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        Random random = new Random();
        List<Tuple3<Integer,String,Integer>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Tuple3<Integer, String,Integer> t = Tuple3.of(i, "name_" + i,random.nextInt(100));
            list.add(t);
        }
        while (flag) {
            List<Tuple3<Integer, String, Integer>> collect = list.stream().map(t -> {
                return Tuple3.of(t.f0, t.f1, t.f2);
            }).collect(Collectors.toList());
            long currentTime = System.currentTimeMillis();

            collect.stream().forEach(t-> {
                ctx.collect(new Student(t.f0,t.f1,t.f2,currentTime));
            });
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}

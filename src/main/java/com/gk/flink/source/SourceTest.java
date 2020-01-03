package com.gk.flink.source;

import com.gk.flink.domain.Student;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description: TODO
 * @Author: GK
 * @Date: 2020/1/3
 */
public class SourceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Student> studentDataStreamSource = env.addSource(new MySourceFunction());
        studentDataStreamSource.print();
        env.execute("Test Defined Source");
    }

}

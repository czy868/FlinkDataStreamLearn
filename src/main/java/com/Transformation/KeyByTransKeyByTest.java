package com.Transformation;

import com.entity.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByTransKeyByTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Mary", "./home1", 1000L),
                new Event("Mary", "./home2", 1000L),
                new Event("Mary", "./home3", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 注意keyBy只能用键值流去处理
//        KeyedStream<Event,String> keyedStream = stream.keyBy(e->e.user);


        // 自定义一个keyby函数,Mary的全部都分区1，其他在2中。分区其实就是并行度的另一层概念
        stream.keyBy(new MyKeySelector()).print().setParallelism(2);


        env.execute();

    }

    public static class MyKeySelector implements KeySelector<Event,String> {

        @Override
        public String getKey(Event event) throws Exception {
            return event.user;
        }
    }
}

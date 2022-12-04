package com.Transformation;

import com.entity.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

public class TransFilterTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 通过实现匿名类的方式
        stream.map(new MapFunction<Event, Object>() {

            @Override
            public Object map(Event event) throws Exception {
                return event.user;
            }
        });

        // 传入Map算子的实现类
        stream.map(new UserExtractor()).print().setParallelism(2);

        // lambel表达式
        stream.map(data->data.user);
        env.execute();

    }

    // 需要实现MapFunction函数
    public static class UserExtractor implements MapFunction<Event,String> {

        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}

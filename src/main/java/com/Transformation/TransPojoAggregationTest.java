package com.Transformation;

import com.entity.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransPojoAggregationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Mary", "./home1", 5000L),
                new Event("Bob", "./cart", 2000L)
        );

        stream.keyBy(value->value.user).maxBy("timestamp").print();

        env.execute();
    }
}

package com.Transformation;

import com.entity.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterTransFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source并行度
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

//        stream.filter(new FilterFunction<Event>() {
//            @Override
//            public boolean filter(Event event) throws Exception {
//                return event.equals("Mary");
//            }
//        }).print();
//
//        stream.filter(new UserFilter()).print();

        stream.filter(data -> data.user.equals("Mary")).print();

        env.execute();
    }

    public static class UserFilter implements FilterFunction<Event> {

        @Override
        public boolean filter(Event event) throws Exception {
            return event.equals("Mary");
        }
    }
}

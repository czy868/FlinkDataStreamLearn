package com.Transformation;

import com.source.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(5,5,5,5,5,5,5,5,5,1,2,2,3,3,3,4,4,4,4).map(
                new MapFunction<Integer, Tuple2<String,Long>>() {

                    @Override
                    public Tuple2<String,Long> map(Integer integer) throws Exception {
                        return Tuple2.of(integer.toString(),1L);
                    }
                }).keyBy(value -> value.f0).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0,value1.f1+value2.f1);
            }
        }).keyBy(t->true).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return value1.f1>value2.f1 ? value1:value2;
            }
        }).print();

        env.execute();
    }
}

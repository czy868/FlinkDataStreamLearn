package com.Transformation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransTupleAggreationTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<Tuple2<String, Integer>> stream = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("b", 3),
                Tuple2.of("b", 4)
        );
        stream.keyBy(r->r.f0).sum(1).print();
        stream.keyBy(r->r.f0).sum("f1").print();
        stream.keyBy(r->r.f0).max(1).print();
        stream.keyBy(r->r.f0).max("f1").print();
        stream.keyBy(r->r.f0).min(1).print();
        stream.keyBy(r->r.f0).min("f1").print();
        // By和不By的区别在于：by在更新我们求最大值的那一列的时候，其他类也会更新为最大值的行的其他列数据，不带By的只更新要求最大值的那一列
        stream.keyBy(r->r.f0).maxBy(1).print();
        stream.keyBy(r->r.f0).maxBy("f1").print();
        stream.keyBy(r->r.f0).minBy(1).print();
        stream.keyBy(r->r.f0).minBy("f1").print();

    }
}

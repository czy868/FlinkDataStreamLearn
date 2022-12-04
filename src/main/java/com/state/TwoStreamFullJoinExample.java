package com.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class TwoStreamFullJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env
                .fromElements(
                        Tuple3.of("a", "stream-1", 1000L),
                        Tuple3.of("b", "stream-1", 2000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String,
                                        Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new
                                                               SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                                                   @Override
                                                                   public long extractTimestamp(Tuple3<String,
                                                                           String, Long> t, long l) {
                                                                       return t.f2;
                                                                   }
                                                               })
                );

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env
                .fromElements(
                        Tuple3.of("a", "stream-2", 3000L),
                        Tuple3.of("b", "stream-2", 4000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String,
                                        Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new
                                                               SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                                                   @Override
                                                                   public long extractTimestamp(Tuple3<String,
                                                                           String, Long> t, long l) {
                                                                       return t.f2;
                                                                   }
                                                               })
                );

        stream1.keyBy(data->data.f0)
                .connect(stream2)
                .process(new MyProcess())
                .print("result");

        env.execute();
    }

    public static class MyProcess extends CoProcessFunction<Tuple3<String, String, Long>,
            Tuple3<String, String, Long>, String>{

        //定义两个集合状态分别存两个流中同key的数据。因为在connect之前就做过了keyby，
        // 所以同一个key的会经过通过一个处理函数
        private ListState<Tuple3<String, String, Long>>
                stream1ListState;
        private ListState<Tuple3<String, String, Long>>
                stream2ListState;


        @Override
        public void processElement1(Tuple3<String, String, Long> stringStringLongTuple3, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
            stream1ListState.add(stringStringLongTuple3);

            for (Tuple3<String, String, Long> value : stream2ListState.get()) {
                collector.collect(stringStringLongTuple3 + "=>" + value);
            }

        }

        @Override
        public void processElement2(Tuple3<String, String, Long> stringStringLongTuple3, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
            stream2ListState.add(stringStringLongTuple3);

            for (Tuple3<String, String, Long> value : stream2ListState.get()) {
                collector.collect(value + "=>" + stringStringLongTuple3);
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            stream1ListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<Tuple3<String, String,
                                                Long>>("stream1-list", Types.TUPLE(Types.STRING, Types.STRING))
            );
            stream2ListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<Tuple3<String, String,
                            Long>>("stream2-list", Types.TUPLE(Types.STRING, Types.STRING))
            );

        }
    }
}

package com.state;

import com.entity.Event;
import com.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class FakeWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new
                        ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new
                                                       SerializableTimestampAssigner<Event>() {
                                                           @Override
                                                           public long extractTimestamp(Event element, long
                                                                   recordTimestamp) {
                                                               return element.timestamp;
                                                           }
                                                       })
                );

        stream.print("source");

        stream.keyBy(data -> data.url)
                .process(new FakeWindowResult(10L))
                .print("result");

        env.execute();
    }

    public static class FakeWindowResult extends KeyedProcessFunction<String, Event, String> {

        // 定义属性，窗口长度
        private Long windowSize;

        public FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        // 声明状态，用 map 保存 pv 值（窗口 start，count）
        MapState<Long, Long> windowPvMapState;
        ValueState<Long> timerTsState;
        ValueState<Long> countSate;


        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
            Long windowStart = event.timestamp / windowSize * windowSize;

            Long windEnd = event.timestamp + windowSize * 1000L;

            context.timerService().registerEventTimeTimer(windEnd - 1);

            if (windowPvMapState.contains(windowStart)) {
                Long pv = windowPvMapState.get(windowStart);
                windowPvMapState.put(windowStart, pv + 1);
            } else {
                windowPvMapState.put(windowStart, 1L);
            }

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            Long windowEnd = timestamp + 1;
            Long windowStart = windowEnd - windowSize * 1000L;
            Long pv = windowPvMapState.get(windowStart);

            out.collect("url: " + ctx.getCurrentKey()
                    + " 访问量: " + pv
                    + " 窗 口 ： " + new Timestamp(windowStart) + " ~ " + new
                    Timestamp(windowEnd));
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            windowPvMapState = getRuntimeContext().getMapState(new
                    MapStateDescriptor<Long, Long>("window-pv", Long.class, Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));

        }
    }
}

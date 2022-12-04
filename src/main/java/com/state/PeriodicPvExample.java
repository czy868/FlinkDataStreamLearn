package com.state;

import com.entity.Event;
import com.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class PeriodicPvExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));


        stream.print("source");
        stream.keyBy(data -> data.user)
                .process(new PeriodicPvResult())
                .print("result");


        env.execute();


    }

    public static class PeriodicPvResult extends KeyedProcessFunction<String, Event, String> {

        ValueState<Long> countSate;
        ValueState<Long> timerTsState;

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
            Long count = countSate.value();
            if (count == null) {
                countSate.update(1L);
            } else {
                countSate.update(count + 1);
            }

            if (timerTsState.value() == null) {
                //如果没有定时器的状态，可以注册一个从当前时间时间开始10秒的定时器
                context.timerService().registerEventTimeTimer(event.timestamp + 10 * 1000L);
                timerTsState.update(event.timestamp + 10 * 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "Pv :" + countSate.value());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            countSate = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));

            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
        }
    }
}

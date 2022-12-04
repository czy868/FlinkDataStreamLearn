package com.stream;

import com.entity.Event;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class BillCheckExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream =
                env.fromElements(
                        Tuple3.of("order-1", "app", 1000L),
                        Tuple3.of("order-2", "app", 2000L)
                ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String,
                                String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String,String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                                return stringStringLongTuple3.f2;
                            }
                        }));



        SingleOutputStreamOperator<Tuple4<String, String, String, Long>>
                thirdpartStream = env.fromElements(
                Tuple4.of("order-1", "third-party", "success", 3000L),
                Tuple4.of("order-2", "third-party", "success", 7000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String,
                String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new
                                                       SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                                                           @Override
                                                           public long extractTimestamp(Tuple4<String, String, String, Long>
                                                                                                element, long recordTimestamp) {
                                                               return element.f3;
                                                           }
                                                       })
        );


        appStream.connect(thirdpartStream)
                .keyBy(data->data.f0,data->data.f0)
                .process(new OrderMatchResult())
                .print("Result");

        env.execute();

    }

    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String,
                String, Long>, Tuple4<String, String, String, Long>, String> {


        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>>
                thirdPartyEventState;


        @Override
        public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，判断状态，如果某个状态不为空，说明另一条流中事件没来，定时器出发完成只会需要清空所有状态
            if (appEventState.value() != null) {
                out.collect("对账失败：" + appEventState.value() + " " + "第三方支付 平台信息未到");
            }
            if (thirdPartyEventState.value() != null) {
                out.collect("对账失败：" + thirdPartyEventState.value() + " " + "app 信息未到");
            }
            appEventState.clear();
            thirdPartyEventState.clear();
        }


        //初始化状态
        @Override
        public void open(Configuration parameters) throws Exception {
            appEventState = getRuntimeContext().getState(
            new ValueStateDescriptor<Tuple3<String, String,
                                Long>>("app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
 );
            thirdPartyEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple4<String, String, String,
                            Long>>("thirdparty-event", Types.TUPLE(Types.STRING, Types.STRING,
                            Types.STRING,Types.LONG))
            );

        }

        @Override
        public void processElement1(Tuple3<String, String, Long> stringStringLongTuple3, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
            if (thirdPartyEventState.value()!= null) {
                collector.collect(" 对 账 成 功 ： " + stringStringLongTuple3 + " " +
                        thirdPartyEventState.value());
                thirdPartyEventState.clear();
            } else {
                appEventState.update(stringStringLongTuple3);
                context.timerService().registerEventTimeTimer(stringStringLongTuple3.f2+5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> stringStringStringLongTuple4, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
            if (appEventState.value() != null){
                collector.collect("对账成功："  + stringStringStringLongTuple4 + appEventState.value() + " ");
                // 清空状态
                appEventState.clear();
            } else {
                // 更新状态
                thirdPartyEventState.update(stringStringStringLongTuple4);
                // 注册一个 5 秒后的定时器，开始等待另一条流的事件
                context.timerService().registerEventTimeTimer(stringStringStringLongTuple4.f3 + 5000L);
            }
        }
    }
}

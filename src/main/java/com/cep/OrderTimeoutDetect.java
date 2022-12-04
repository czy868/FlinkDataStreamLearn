package com.cep;

import com.entity.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

//对超时事件的处理
public class OrderTimeoutDetect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 获取订单事件流，并提取时间戳、生成水位线
        KeyedStream<OrderEvent, String> stream = env
                .fromElements(
                        new OrderEvent("user_1", "order_1", "create", 1000L),
                        new OrderEvent("user_2", "order_2", "create", 2000L),
                        new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                        new OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
                        new OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
                        new OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new
                                                SerializableTimestampAssigner<OrderEvent>() {
                                                    @Override
                                                    public long extractTimestamp(OrderEvent
                                                                                         event, long l) {
                                                        return event.timestamp;
                                                    }
                                                }
                                )
                )
                .keyBy(order -> order.orderId);

        //定义一个模式：第一个动作是创建订单的用户，第二是之前创建订单的用户开始支付订单。与此同时手机哪些创建订单然后超过15分钟还迟迟未下单的用户信息
        Pattern<OrderEvent,?> pattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.eventType.equals("create");
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.eventType.equals("pay");
                    }
                })
                .within(Time.minutes(15));

        PatternStream<OrderEvent> pattern1 = CEP.pattern(stream, pattern);

        //定义的侧输出流用来接收哪些超时的数据
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};

        SingleOutputStreamOperator<String> process = pattern1.process(new OrderPayPatternProcessFunction());

        process.print("normal");
        //将数据输出到输出流中进行打印
        process.getSideOutput(timeoutTag).print("tinmout");

        env.execute();

    }

    /**
     * 自定义的超时数据处理类
     */
    public static class OrderPayPatternProcessFunction extends PatternProcessFunction<OrderEvent,String>  implements TimedOutPartialMatchHandler<OrderEvent> {

        @Override
        public void processMatch(Map<String, List<OrderEvent>> map, Context context, Collector<String> collector) throws Exception {
            OrderEvent payEvent = map.get("pay").get(0);
            collector.collect("订单 " + payEvent.orderId + " 已支付！");
        }

        /**
         * 超时数据会进入到这个方法
         * @param map
         * @param context
         * @throws Exception
         */
        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> map, Context context) throws Exception {
            OrderEvent createEvent = map.get("create").get(0);
            context.output(new OutputTag<String>("timeout"){}, "订单 " +
                    createEvent.orderId + " 超时未支付！用户为：" + createEvent.userId);

        }
    }
}

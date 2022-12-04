package com.Transformation;

import com.entity.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionTest {
    // 富函数区别与其他函数在于，多了一些方法，比如open是在算子创建开始必定会初始化的，受并行度的影响
    // close是算子结束必定会调用的方法。
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L)
        );

        clicks.map(new RichMapFunction<Event, Object>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println(" 索 引 为 " +
                        getRuntimeContext().getIndexOfThisSubtask() + " 的任务开始");
            }

            @Override
            public Object map(Event event) throws Exception {
                 return event.timestamp;
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println(" 索 引 为 " +
                        getRuntimeContext().getIndexOfThisSubtask() + " 的任务结束");
            }
        }).print();

        env.execute();
    }
}

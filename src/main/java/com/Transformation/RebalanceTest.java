package com.Transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RebalanceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
// 读取数据源，并行度为 1
        DataStreamSource<Integer> stream = env.fromElements(1,2,3,4,5,6,7,8);

        stream.rebalance().print().setParallelism(2);
        env.execute();
    }
}

package com.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

// 可并行的数据源头
public class ParallelSourceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        executionEnvironment.addSource(new CustomSource()).setParallelism(2);
        executionEnvironment.execute();
    }

    public static class CustomSource implements ParallelSourceFunction<Integer> {

        //数据截停的标志位置
        private boolean running = true;
        @Override
        public void run(SourceContext<Integer> sourceContext) throws Exception {
            //产生数据的方法，一般是链接组件消费数据
            while (running) {
                sourceContext.collect(1);
            }
        }

        @Override
        public void cancel() {
            //该表标志位置，截停数据
            running  = false;
        }
    }
}

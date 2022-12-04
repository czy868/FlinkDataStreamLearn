package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.stream.Collectors;

public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1.创建一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取数据源
        DataStreamSource<String> lineDSS = env.readTextFile("input/words.txt");

        SingleOutputStreamOperator<Tuple2<String,Long>> wordAndOne = lineDSS
                .flatMap((String line, Collector<String> words)->{
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                }).returns(Types.STRING).map(word->Tuple2.of(word,1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        KeyedStream<Tuple2<String,Long>,String> wordAndOneKS = wordAndOne.keyBy(t->t.f0);
        SingleOutputStreamOperator<Tuple2<String,Long>> result = wordAndOneKS.sum(1);
        result.print();
        env.execute();
    }

}

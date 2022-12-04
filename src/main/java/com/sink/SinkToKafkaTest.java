package com.sink;

import com.entity.Event;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SinkToKafkaTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("Mary",
                        "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));

        // 1、设置写入的topic
        // 2、设置写入数据的相关模板，后续读取kafka数据的情况需要考虑到
        // kafkaconnect的其他配置
        FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<String>(
                "clicks",
                new SimpleStringSchema(),
                new Properties()
        );
        stream.map(Event::getUser).addSink(flinkKafkaProducer);

        env.execute();

    }
}

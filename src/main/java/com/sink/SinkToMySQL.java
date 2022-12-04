package com.sink;

import com.entity.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class SinkToMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));

        //1、插入数据模板
        //2、配置参数
        // jbdc链接参数
        stream.addSink(JdbcSink.sink(
                "INSERT INTO clicks (user, url) VALUES (?, ?)",
                (statement,r) ->{
                    statement.setString(1,r.user);
                    statement.setString(1,r.url);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/userbehavior")
                                        // 对于 MySQL 5.7，用"com.mysql.jdbc.Driver"
                                        .withDriverName("com.mysql.cj.jdbc.Driver")
                                        .withUsername("username")
                                        .withPassword("password")
                                        .build()

                        ));
    env.execute();
    }
}

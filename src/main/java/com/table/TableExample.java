package com.table;

import com.entity.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class TableExample {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 读取数据源
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        // 注册表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //讲数据源转化为一个表
        Table eventTable = tableEnv.fromDataStream(eventStream);

        //基于上面的表做查询，查询出来的数据转化为一个临时表存起来，from后面要加空格吗，就和sql拼接一样的
//        Table table = tableEnv.sqlQuery("select url, user from " + eventTable);

        //另一种从表中取数据的方式
        Table select = eventTable.select($("url"), $("user"));
        //将表转化为数据流输出
        tableEnv.toChangelogStream(select).print("Table");

        env.execute();
    }
}

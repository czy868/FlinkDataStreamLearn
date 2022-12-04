package com.table;

import com.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

//窗口函数实现TopN
public class WindowTopNExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并分配时间戳、生成水位线
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                        new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                        new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                        new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                        new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new
                                                               SerializableTimestampAssigner<Event>() {
                                                                   @Override
                                                                   public long extractTimestamp(Event element, long
                                                                           recordTimestamp) {
                                                                       return element.timestamp;
                                                                   }
                                                               })
                );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table eventTable = tableEnv.fromDataStream(eventStream, $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts")
// 将 timestamp 指定为事件时间，并命名为 ts
        );

        tableEnv.createTemporaryView("EventTable", eventTable);

        //先按照用户喝窗口做聚合
        String sql = "select window_start,window_end,user,count(1) as cnt " +
                "from TABLE( " +
                "TUMBLE(TABLE EventTable ,DESCRIPTOR(ts),INTERVAL '1' HOUR) " +
                ") " +
                "group by window_start,window_end,user ";

        Table table = tableEnv.sqlQuery(sql);

        //在对应的窗口内，求ROW_NUMBER函数
        String sql1 = "select * from ( " +
                "select *, " +
                "ROW_NUMBER() OVER( " +
                "PARTITION BY window_start, window_end " +
                "order by cnt desc " +
                ") AS row_num " +
                "from " + table + ") " +
                "where row_num <=2 ";

        Table table1 = tableEnv.sqlQuery(sql1);

        tableEnv.toChangelogStream(table1).print("result");

        env.execute();

    }
}

package com.stream;

import com.entity.Event;
import com.source.ClickSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SplitStreamByOutputTag {


    public static OutputTag<Tuple3<String,String,Long>> MaryTag = new OutputTag<Tuple3<String,String,Long>>("Mary-pv"){};
    public static OutputTag<Tuple3<String,String,Long>> BobTag = new OutputTag<Tuple3<String,String,Long>>("BobTag-pv"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> process = streamSource.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, Event>.Context context, Collector<Event> collector) throws Exception {
                if (event.user.equals("Mary")) {
                    context.output(MaryTag, new Tuple3<>(event.user, event.url, event.timestamp));
                } else if (event.user.equals("Bob")) {
                    context.output(BobTag, new Tuple3<>(event.user, event.url, event.timestamp));
                } else {
                    collector.collect(event);
                }
            }
        });

        process.getSideOutput(MaryTag).print("Mary");
        process.getSideOutput(BobTag).print("Bob");

        process.print("Result");
        env.execute();
    }
}

package com.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

// 首先watermark之和eventTime有关系，event time本身代表的是元素自带的时间属性，一般是时间发生的时间。
public class BoundedOutOfOrdernessGenerator implements WatermarkGenerator<String> {

    private final long maxOutOfOrderness = 3500; // 3.5 秒

    private long currentMaxTimestamp;

    @Override
    public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // 发出的 watermark = 当前最大时间戳 - 最大乱序时间
        output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
    }

}

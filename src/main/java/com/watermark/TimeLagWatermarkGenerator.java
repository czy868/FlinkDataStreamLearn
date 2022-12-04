package com.watermark;

import com.entity.User;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class TimeLagWatermarkGenerator implements WatermarkGenerator<User> {
    private final long maxTimeLag = 5000;
    @Override
    public void onEvent(User user, long l, WatermarkOutput watermarkOutput) {
        // 不需要处理
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        watermarkOutput.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag));
    }
}

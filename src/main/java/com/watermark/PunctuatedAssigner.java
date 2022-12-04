package com.watermark;

import com.entity.User;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

//自定义的Watermark生成器，当事件带有某个标记的时候，生成器就会发出watermark
public class PunctuatedAssigner implements WatermarkGenerator<User> {

    @Override
    public void onEvent(User user, long l, WatermarkOutput watermarkOutput) {
        if (user.getId() == 1) {
            watermarkOutput.emitWatermark(new Watermark(l));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

    }
}

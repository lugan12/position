package com.lugan.flink.trigger.buildin;


import com.lugan.flink.trigger.AggregatePrinter;
import com.lugan.flink.trigger.Device;
import com.lugan.flink.trigger.DeviceTransformer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;

/**
 * CountTrigger示例
 * 指定数量，当窗口内数据数量到达该数量之后触发
 *
 * @author shirukai
 */
public class CountTriggerExample {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 时间语义设置为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 3. 创建一个socket数据源
        DataStream<String> socketSource = env.socketTextStream("192.168.25.151", 7777);

        DataStream<String> deviceEventDataStream = socketSource
                // 4. 将数据源String类型数据格式化为Device
                .map(new DeviceTransformer())
                // 5. 分发事件时间和水印
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Device>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(Device element) {
                        return element.getTimestamp();
                    }
                })
                // 6. 按照id进行分组
                .keyBy("id")
                // 7. 设置1分钟的滚动窗口
                .timeWindow(Time.seconds(5))
                // 8. 设置触发器
                .trigger(EventTimeTrigger.create())
                // 9. 自定义聚合器
                .process(new AggregatePrinter());

        // 10. 结果输出控制台
        deviceEventDataStream.print("Print");

        // 11. 提交执行
        env.execute("CountTriggerExample");

    }
}

package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * ClassName: Flink08_Window_EventTime_Tumbling_Bounded_WaterMark_Late_OutPut
 * Package: com.atguigu.day05
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/11 1:28
 * @Version 1.0
 */
public class Flink08_Window_EventTime_Tumbling_Bounded_WaterMark_Late_OutPut {
    public static void main(String[] args) throws Exception {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.对数据进行处理，封装成WaterSensor
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //TODO 指定WaterMark
        SingleOutputStreamOperator<WaterSensor> timestampsAndWatermarks = waterSensorDStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        //TODO 指定有乱序程度的WaterMark->乱序程度为2s
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            //TODO 分配时间戳
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs() * 1000;
                            }
                        })
        );

        //4.将相同元素的数据聚和到一块
        KeyedStream<WaterSensor, String> keyedStream = timestampsAndWatermarks.keyBy(WaterSensor::getId);

        //5.开启基于事件时间的滚动窗口
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //TODO 设置允许迟到时间3S
                .allowedLateness(Time.seconds(3))
                //TODO 设置侧输出流 ->真正迟到的数据
                .sideOutputLateData(new OutputTag<WaterSensor>("sideoutput") {});

        //6.处理
        SingleOutputStreamOperator<String> process = window.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg = "当前key: " + key
                        + "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                        + elements.spliterator().estimateSize() + "条数据 ";
                out.collect(msg);
            }
        });

        process.print("主流：");
        //TODO 获取侧输出流的内容
        process.getSideOutput(new OutputTag<WaterSensor>("sideoutput") {}).print("侧流：");

        env.execute();

    }
}

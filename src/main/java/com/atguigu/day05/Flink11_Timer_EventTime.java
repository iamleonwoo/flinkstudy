package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * ClassName: Flink11_Timer_EventTime
 * Package: com.atguigu.day05
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/11 2:50
 * @Version 1.0
 */
public class Flink11_Timer_EventTime {
    public static void main(String[] args) throws Exception {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                //1.注册基于事件时间的定时器
                System.out.println("注册一个5秒定时器");
                ctx.timerService().registerEventTimeTimer(value.getTs() * 1000 + 5000);
            }

            // 这个方法指的是定时器到点后，要干的事
            // 参数timestamp: 定时器被触发的时间
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect( "闹钟在" + timestamp + "响了！！！");
            }
        }).print();

        env.execute();

    }
}

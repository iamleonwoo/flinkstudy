package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * ClassName: Flink09_OutPut_Exe
 * Package: com.atguigu.day05
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/11 1:49
 * @Version 1.0
 */
public class Flink09_OutPut_Exe {
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

        //4.将相同元素的数据聚和到一块
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDStream.keyBy(WaterSensor::getId);

        //TODO 采集监控传感器水位值，将水位值高于5cm的值输出到side output
        SingleOutputStreamOperator<WaterSensor> process = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                if (value.getVc() <= 5) {
                    out.collect(value);
                } else {
                    ctx.output(new OutputTag<WaterSensor>("over5cm") {
                    }, value);
                }
            }
        });

        process.print("主流：");
        process.getSideOutput(new OutputTag<WaterSensor>("over5cm"){}).print("侧流：");

        env.execute();

    }
}

package com.atguigu.day03.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: Flink03_TransForm_Agg
 * Package: com.atguigu.day03.transform
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/7 21:13
 * @Version 1.0
 */
public class Flink03_TransForm_Agg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //将获取到的数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //将相同id的数据聚和到一块
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDStream.keyBy(value -> value.getId());

        //TODO Max
//        keyedStream.max("vc").print();
        keyedStream.maxBy("vc", true).print();

        env.execute();

    }
}

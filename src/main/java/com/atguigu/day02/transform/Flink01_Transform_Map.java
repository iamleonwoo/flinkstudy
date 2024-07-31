package com.atguigu.day02.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: Flink01_Transform_Map
 * Package: com.atguigu.day02.transform
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/7 0:55
 * @Version 1.0
 */
public class Flink01_Transform_Map {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //TODO 3.使用map对数据进行转换，将读进来的数据封装成WaterSensor
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] data = value.split(",");
                return new WaterSensor(data[0], Long.parseLong(data[1]), Integer.parseInt(data[2]));
            }
        });

        map.print();

        //4.执行任务
        env.execute();

    }
}

package com.atguigu.day02.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: Flink05_Transform_KeyBy
 * Package: com.atguigu.day02.transform
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/7 1:42
 * @Version 1.0
 */
public class Flink05_Transform_KeyBy {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

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

        //TODO 4.使用keyby按照WaterSensor的id进行分组
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");

        keyedStream.print();

        //5.执行任务
        env.execute();

    }

}

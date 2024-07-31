package com.atguigu.day03.transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * ClassName: Flink01_Transform_Connect
 * Package: com.atguigu.day03.transform
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/7 20:50
 * @Version 1.0
 */
public class Flink01_Transform_Connect {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据源
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);

        DataStreamSource<String> source2 = env.fromElements("a", "b", "c", "d", "e", "f");

        //TODO 3.使用Connect连接两条流
        ConnectedStreams<Integer, String> connectedStreams = source1.connect(source2);

        SingleOutputStreamOperator<String> map = connectedStreams.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "source1: " + value;
            }

            @Override
            public String map2(String value) throws Exception {
                return "source2: " + value;
            }
        });

        map.print();

        env.execute();
    }
}

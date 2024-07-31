package com.atguigu.day03.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: Flink02_Transform_Union
 * Package: com.atguigu.day03.transform
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/7 21:00
 * @Version 1.0
 */
public class Flink02_Transform_Union {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据源
        DataStreamSource<String> source1 = env.fromElements("1", "2", "3", "4", "5", "6");

        DataStreamSource<String> source2 = env.fromElements("a", "b", "c", "d");

        //TODO 3.使用Union连接两条流
        DataStream<String> union = source1.union(source2);

        union.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();

        env.execute();
    }
}

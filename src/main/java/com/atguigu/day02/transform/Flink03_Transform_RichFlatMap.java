package com.atguigu.day02.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * ClassName: Flink03_Transform_RichFlatMap
 * Package: com.atguigu.day02.transform
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/7 1:09
 * @Version 1.0
 */
public class Flink03_Transform_RichFlatMap {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取数据
//        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
//        List<String> list = Arrays.asList("s1,1,1", "s2,2,2");
//        DataStreamSource<String> streamSource = env.fromCollection(list);
        DataStreamSource<String> streamSource = env.readTextFile("input/waterSensor.txt");

        //TODO 3.FlatMap
        streamSource.flatMap(new MyFlatMap()).print();

        //4.执行任务
        env.execute();

    }

    //自定义一个类，来实现富函数的抽象类
    public static class MyFlatMap extends RichFlatMapFunction<String,String>{
        /**
         * 生命周期，会在启动时调用，每个并行实例调用一次
         * @param parameters The configuration containing the parameters attached to the contract.
         *
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open......");
        }


        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] data = value.split(",");
//            System.out.println(getRuntimeContext().getTaskName());
            for (String datum : data) {
                out.collect(datum);
            }
        }

        /**
         * 生命周期程序结束时调用，每个并行实例调用一次,注意！！！！读文件时每个并行度会调用两次
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            System.out.println("close......");
        }
    }
}

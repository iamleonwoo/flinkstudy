package com.atguigu.day02.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * ClassName: Flink01_Source_Collection
 * Package: com.atguigu.day02.source
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/6 23:25
 * @Version 1.0
 */
public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.创建集合
        List<String> list = Arrays.asList("1", "2", "3", "4");

        //TODO 3.从集合中获取数据
        DataStreamSource<String> streamSource = env.fromCollection(list);
        streamSource.print();

//        DataStreamSource<String> streamSource2 = env.fromElements("a", "b", "c", "d");
//        streamSource2.print();

        //4.执行任务
        env.execute();

    }
}

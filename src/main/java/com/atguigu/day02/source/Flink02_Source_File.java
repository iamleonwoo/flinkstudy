package com.atguigu.day02.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: Flink02_Source_File
 * Package: com.atguigu.day02.source
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/6 23:45
 * @Version 1.0
 */
public class Flink02_Source_File {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.从文件中获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");

        streamSource.print();

        //3.执行任务
        env.execute();

    }
}

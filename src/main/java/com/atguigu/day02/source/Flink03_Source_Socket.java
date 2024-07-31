package com.atguigu.day02.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: Flink03_Source_Socket
 * Package: com.atguigu.day02.source
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/6 23:55
 * @Version 1.0
 */
public class Flink03_Source_Socket {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.从Socket中获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        streamSource.print();

        //3.执行任务
        env.execute();

    }
}

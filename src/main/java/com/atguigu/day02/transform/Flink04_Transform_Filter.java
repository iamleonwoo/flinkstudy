package com.atguigu.day02.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: Flink04_Transform_Filter
 * Package: com.atguigu.day02.transform
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/7 1:35
 * @Version 1.0
 */
public class Flink04_Transform_Filter {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //TODO 3.使用Filter对数据进行过滤，把偶数过滤掉
        streamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return Integer.parseInt(value) % 2 != 0;
            }
        }).print();

        //4.执行任务
        env.execute();
    }
}

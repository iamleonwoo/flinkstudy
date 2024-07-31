package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;


/**
 * ClassName: Flink02_Project_PV_Process
 * Package: com.atguigu.day04
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/9 21:27
 * @Version 1.0
 */
public class Flink02_Project_PV_Process {
    public static void main(String[] args) throws Exception {

        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件中获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        //3.使用process实现pv统计
        streamSource.process(new ProcessFunction<String, Tuple2<String,Long>>() {
            //定义一个累加器
            Long count = 0L;

            @Override
            public void processElement(String value, ProcessFunction<String, Tuple2<String, Long>>.Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                //1.切分数据，拿到pv的数据
                String[] split = value.split(",");
                //2.过滤出pv
                if ("pv".equals(split[3])) {
                    count++;
                    out.collect(Tuple2.of(split[3],count));
                }
            }
        }).print();

        env.execute();

    }
}

package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;

/**
 * ClassName: Flink03_Project_UV
 * Package: com.atguigu.day04
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/9 21:41
 * @Version 1.0
 */
public class Flink03_Project_UV {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setParallelism(2);

        //2.从文件中获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        //3.将数据转为javaBean
        SingleOutputStreamOperator<UserBehavior> map = streamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                //将数据按照逗号切分
                String[] split = value.split(",");

                UserBehavior userBehavior = new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4])
                );

                return userBehavior;

            }
        });

        //4.将pv的数据过滤出来
        SingleOutputStreamOperator<UserBehavior> filter = map.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });

        //5.由于需要保证即使多并行度下结果的正确性，因此要对数据进行keyby使数据全部进入同一分区，所以先将数据转为Tuple
        SingleOutputStreamOperator<Tuple2<String, Long>> map1 = filter.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return Tuple2.of("pv", value.getUserId());
            }
        });

        //6.将数据聚和起来
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = map1.keyBy(0);

        //7.累加计算
        keyedStream.process(new ProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
            HashSet<Long> set = new HashSet<>();

            @Override
            public void processElement(Tuple2<String, Long> value, ProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>>.Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                //TODO 里面来一条数据调用一次
                //1.去重
                set.add(value.f1);
                //2.将结果数据发送给下游
                out.collect(Tuple2.of("uv", (long) set.size()));
            }
        }).print();

        env.execute();

    }
}

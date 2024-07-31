package com.atguigu.day04;

import com.atguigu.bean.AdsClickLog;
import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * ClassName: Flink06_Project_Ads_Click
 * Package: com.atguigu.day04
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/9 22:55
 * @Version 1.0
 */
public class Flink06_Project_Ads_Click {
    public static void main(String[] args) throws Exception {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/AdClickLog.csv");
        //3.处理数据
        SingleOutputStreamOperator<Tuple2<String, Long>> map = streamSource.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                AdsClickLog adsClickLog = new AdsClickLog(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        split[2],
                        split[3],
                        Long.parseLong(split[4])
                );
                return Tuple2.of(adsClickLog.getProvince() + "-" + adsClickLog.getAdId(), 1L);
            }
        });

        //4.将数据聚合起来
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = map.keyBy(0);

        //5.累加
        keyedStream.sum(1).print();

        env.execute();

    }

}

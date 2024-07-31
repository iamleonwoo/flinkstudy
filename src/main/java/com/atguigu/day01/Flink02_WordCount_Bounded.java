package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ClassName: Flink02_WordCount_Bounded
 * Package: com.atguigu.day01
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/4 22:40
 * @Version 1.0
 */
public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        //1.获取流处理执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //将线程数设置为1
        senv.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = senv.readTextFile("input/word.txt");

        //3.将读过来的数据按照空格切分，切成一个一个单词,并组成Tuple元组
        SingleOutputStreamOperator<Tuple2<String, Long>> word2One = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                //a.按照空格切分
                String[] words = value.split(" ");
                //b.遍历数组取出数据
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

        //4.将相同key的数据聚和到一块
        KeyedStream<Tuple2<String, Long>, Tuple> keyByWord = word2One.keyBy(0);

        //5.做累加计算
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyByWord.sum(1);

        //6.输出到控制台
        result.print();

        //7.执行程序
        senv.execute();

    }
}

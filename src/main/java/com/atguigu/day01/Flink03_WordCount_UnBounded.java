package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ClassName: Flink03_WordCount_UnBounded
 * Package: com.atguigu.day01
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/4 22:54
 * @Version 1.0
 */
public class Flink03_WordCount_UnBounded {
    public static void main(String[] args) throws Exception {
        //1.获取流处理执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        senv.setParallelism(1);

        //2.获取无界的数据
        DataStreamSource<String> streamSource = senv.socketTextStream("hadoop102", 9999);

        //3.将读过来的数据按照空格切分，切分成一个一个单词
        SingleOutputStreamOperator<String> word = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //4.将单词组成Tuple元组
        SingleOutputStreamOperator<Tuple2<String, Long>> word2One = word.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return Tuple2.of(value, 1L);
            }
        });

        //5.将相同key的数据聚和到一块
        KeyedStream<Tuple2<String, Long>, String> keyByWord = word2One.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });

        //6.做累加操作
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyByWord.sum(1);

        //7.打印到控制台
        result.print();

        //8.执行程序
        senv.execute();

    }
}

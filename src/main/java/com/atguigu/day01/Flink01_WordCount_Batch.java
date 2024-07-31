package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * ClassName: Flink01_WordCount_Batch
 * Package: com.atguigu.day01
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/4 16:17
 * @Version 1.0
 */
public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        //1.创建批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.读取数据
        DataSource<String> dataSource = env.readTextFile("input/word.txt");
        //3.flatMap（将一行数据切成一个一个单词）->map(将一个一个的单词组成tuple元组)->reduceByKey（将相同key的数据聚和到一块做累加计算）->输出计算结果
        FlatMapOperator<String, String> word = dataSource.flatMap(new MyFlatMapFun());
        //4.map(将一个一个的单词组成tuple元组)
        MapOperator<String, Tuple2<String, Long>> word2One = word.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return Tuple2.of(value, 1L);
            }
        });
//        MapOperator<String, Tuple2<String, Long>> wordToOneDStream = word.map((MapFunction<String, Tuple2<String, Long>>) value -> Tuple2.of(value, 1L)).returns(Types.TUPLE(Types.STRING,Types.LONG));

        //5.reduceByKey（将相同key的数据聚和到一块做累加计算）
        //5.1将相同key的数据聚和到一块
        UnsortedGrouping<Tuple2<String, Long>> groupByWord = word2One.groupBy(0);

        //5.2做累加操作
        AggregateOperator<Tuple2<String, Long>> result = groupByWord.sum(1);

        //6.将结果输出到控制台
        result.print();
    }

    //自定义类实现FlatMapFun接口
    public static class MyFlatMapFun implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            //1.将读过来的一行数据按照空格切分，切分成一个个单词
            String[] words = value.split(" ");
            //2.遍历数据取出每一个单词
            for (String word : words) {
                out.collect(word);
            }
        }
    }
}

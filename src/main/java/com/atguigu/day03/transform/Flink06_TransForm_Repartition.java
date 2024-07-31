package com.atguigu.day03.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: Flink06_TransForm_Repartition
 * Package: com.atguigu.day03.transform
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/7 22:00
 * @Version 1.0
 */
public class Flink06_TransForm_Repartition {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        //从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<String> map = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).setParallelism(2);

        //TODO KeyBy
        KeyedStream<String, String> keyedStream = map.keyBy(r -> r);

        //TODO Shuffle
        DataStream<String> shuffle = map.shuffle();

        //TODO Rebalance
        DataStream<String> rebalance = map.rebalance();

        //TODO Rescale
        DataStream<String> rescale = map.rescale();

        map.print("原始数据:").setParallelism(2);
        keyedStream.print("KeyBy:");
        shuffle.print("Shuffle:");
        rebalance.print("Rebalance:");
        rescale.print("Rescale:");

        env.execute();
    }
}

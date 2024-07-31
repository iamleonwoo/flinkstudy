package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * ClassName: Flink09_UDF_TableAggFun
 * Package: com.atguigu.day09
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/16 0:36
 * @Version 1.0
 */
public class Flink09_UDF_TableAggFun {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        //将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.将流转为动态表
        Table inputTable = tableEnv.fromDataStream(waterSensorStream);

        //TODO 4.1 不注册函数直接使用TableAPI方式
//        inputTable
//                .groupBy($("id"))
//                .flatAggregate(call(MyTop2Function.class, $("vc")))
//                .select($("id"), $("f0"), $("f1")).execute().print();
//        inputTable
//                .groupBy($("id"))
//                .flatAggregate(call(MyTop2Function.class, $("vc")).as("value","rank"))
//                .select($("id"), $("value"), $("rank")).execute().print();

        //TODO 4.2 先注册
        tableEnv.createTemporarySystemFunction("myTop2Func", MyTop2Function.class);
        //TODO 4.2.1 再使用TableAPI方式
        inputTable
                .groupBy($("id"))
                .flatAggregate(call("myTop2Func", $("vc")).as("value", "rank"))
                .select($("id"), $("value"), $("rank")).execute().print();

    }

    //自定义一个累加器
    public static class MyTop2Accumulator {
        public Integer first;
        public Integer second;
    }

    //自定义表函数，求Top2
    public static class MyTop2Function extends TableAggregateFunction<Tuple2<Integer, String>, MyTop2Accumulator> {

        //初始化累加器
        @Override
        public MyTop2Accumulator createAccumulator() {
            MyTop2Accumulator acc = new MyTop2Accumulator();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            return acc;
        }

        //累加操作
        public void accumulate(MyTop2Accumulator acc, Integer value){
            if (value >= acc.first){
                acc.second = acc.first;
                acc.first = value;
            } else if (value >= acc.second) {
                acc.second = value;
            }
        }

        public void emitValue(MyTop2Accumulator acc, Collector<Tuple2<Integer,String>> out){
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, "1"));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, "2"));
            }
        }
    }
}

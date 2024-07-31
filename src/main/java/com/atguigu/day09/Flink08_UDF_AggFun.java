package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * ClassName: Flink08_UDF_AggFun
 * Package: com.atguigu.day09
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/16 0:16
 * @Version 1.0
 */
public class Flink08_UDF_AggFun {
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
//                .select($("id"), call(MyAvgFun.class, $("vc")).as("vcAvg")).execute().print();

        //TODO 4.2 先注册
        tableEnv.createTemporarySystemFunction("myAvg", MyAvgFun.class);
        //TODO 4.2.1 再使用TableAPI方式
//        inputTable
//                .groupBy($("id"))
//                .select($("id"), call("myAvg", $("vc")).as("avgVc")).execute().print();
        //TODO 4.2.2 再使用SQL方式
        tableEnv.executeSql("select id, myAvg(vc) from " + inputTable +" group by id").print();

    }

    //自定义一个累加器
    public static class MyAccumulator{
        public Integer vcSum;
        public Integer count;

    }

    //自定义表函数，求平均数
    public static class MyAvgFun extends AggregateFunction<Double, MyAccumulator> {

        //初始化累加器
        @Override
        public MyAccumulator createAccumulator() {
            MyAccumulator acc = new MyAccumulator();
            acc.vcSum = 0;
            acc.count = 0;
            return acc;
        }

        //累加操作
        public void accumulate(MyAccumulator accumulator, Integer value){
            accumulator.vcSum += value;
            accumulator.count++;
        }

        //获取最终结果
        @Override
        public Double getValue(MyAccumulator accumulator) {
            return accumulator.vcSum * 1D / accumulator.count;
        }
    }
}

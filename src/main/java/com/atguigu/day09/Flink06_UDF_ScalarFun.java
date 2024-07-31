package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * ClassName: Flink06_UDF_ScalarFun
 * Package: com.atguigu.day09
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/15 23:36
 * @Version 1.0
 */
public class Flink06_UDF_ScalarFun {
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
//        inputTable.select($("id"), call(MyUDFunction.class, $("id"))).execute().print();

        //TODO 4.2 先注册
        tableEnv.createTemporarySystemFunction("myUDF", MyUDFunction.class);
        //TODO 4.2.1 再使用TableAPI方式
//        inputTable.select($("id"), call("myUDF", $("id"))).execute().print();
        //TODO 4.2.2 再使用SQL方式
        tableEnv.executeSql("select id, myUDF(id) from " + inputTable).print();

    }

    //自定义标量函数，统计id的长度
    public static class MyUDFunction extends ScalarFunction{
        public Integer eval(String value){
            return value.length();
        }
    }
}

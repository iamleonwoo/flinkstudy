package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * ClassName: Flink07_UDF_TableFun
 * Package: com.atguigu.day09
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/15 23:54
 * @Version 1.0
 */
public class Flink07_UDF_TableFun {
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
//                .leftOuterJoinLateral(call(MyTableFunction.class, $("id")))
//                .select($("id"), $("word")).execute().print();

        //TODO 4.2 先注册
        tableEnv.createTemporarySystemFunction("myUDTF", MyTableFunction.class);
        //TODO 4.2.1 再使用TableAPI方式
//        inputTable
//                .leftOuterJoinLateral(call("myUDTF", $("id")))
//                .select($("id"), $("word")).execute().print();
        //TODO 4.2.2 再使用SQL方式
        tableEnv.executeSql("select id, word from " + inputTable + " join lateral table(myUDTF(id)) on true").print();
//        tableEnv.executeSql("select id, word from " + inputTable + ", lateral table(myUDTF(id))").print();

    }

    //自定义表函数，按照下划线切分id
    @FunctionHint(output = @DataTypeHint("Row<word STRING>"))
    public static class MyTableFunction extends TableFunction<Row> {
        public void eval(String value) {
            String[] split = value.split("_");
            for (String s : split) {
                collect(Row.of(s));
            }
        }
    }
}

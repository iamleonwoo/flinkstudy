package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * ClassName: Flink01_TableAPI_Demo
 * Package: com.atguigu.day08
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/14 21:17
 * @Version 1.0
 */
public class Flink01_TableAPI_Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorDStream = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        //TODO 获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 将流转为动态表
        Table inputTable = tableEnv.fromDataStream(waterSensorDStream);

        // select * from table where id = sensor_1;
        //TODO 查询表
        Table resultTable = inputTable.where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));

        //TODO 将动态表转为流
        DataStream<Row> resultDStream = tableEnv.toAppendStream(resultTable, Row.class);

        resultDStream.print();


        env.execute();


    }
}

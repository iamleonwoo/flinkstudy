package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * ClassName: Flink01_TableAPI_OverWindow
 * Package: com.atguigu.day09
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/15 22:39
 * @Version 1.0
 */
public class Flink01_TableAPI_OverWindow {
    public static void main(String[] args) {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorStream =
                env.fromElements(
                                new WaterSensor("sensor_1", 1000L, 10),
                                new WaterSensor("sensor_1", 2000L, 20),
                                new WaterSensor("sensor_2", 3000L, 30),
                                new WaterSensor("sensor_1", 4000L, 40),
                                new WaterSensor("sensor_1", 5000L, 50),
                                new WaterSensor("sensor_2", 6000L, 60))
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }));

        //2.创建表的环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.1将流转为表，并指定事件时间
        Table inputTable = tableEnv.fromDataStream(waterSensorStream, $("id"), $("ts").rowtime(), $("vc"));

        //TODO 3.进行开窗操作 求VC的和
        Table table = inputTable
                //上无边界到当前行
//                .window(Over.partitionBy($("id")).orderBy($("ts")).as("w"))
                //从当前行往前两秒钟
//                .window(Over.partitionBy($("id")).orderBy($("ts")).preceding(lit(2).second()).as("w"))
                //从当前行往前两个元素
                .window(Over.partitionBy($("id")).orderBy($("ts")).preceding(rowInterval(2L)).as("w"))
                .select($("id"), $("ts"), $("vc").sum().over($("w")));

        table.execute().print();

    }
}

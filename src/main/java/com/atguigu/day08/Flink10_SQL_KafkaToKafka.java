package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: Flink10_SQL_KafkaToKafka
 * Package: com.atguigu.day08
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/15 0:08
 * @Version 1.0
 */
public class Flink10_SQL_KafkaToKafka {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.创建表kafkaSource表
        tableEnv.executeSql(
                "create table source_sensor (`id` STRING, `ts` BIGINT, `vc` INT) " +
                        "with (" +
                        "'connector' = 'kafka'," +
                        "'topic' = 'topic_source_sensor'," +
                        "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092'," +
                        "'properties.group.id' = 'testGroup2024'," +
                        "'scan.startup.mode' = 'earliest-offset'," +
                        "'format' = 'csv'" +
                        ")"
        );

        //创建KafkaSink表
        tableEnv.executeSql(
                "create table sink_sensor(id string, ts bigint, vc int) " +
                        "with("
                        + "'connector' = 'kafka',"
                        + "'topic' = 'topic_sink_sensor',"
                        + "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',"
                        + "'format' = 'csv'"
                        + ")"
        );

        //将kafka中topic数据写到另一个topic
        tableEnv.executeSql("insert into sink_sensor select * from source_sensor where id = 's1'");

    }
}

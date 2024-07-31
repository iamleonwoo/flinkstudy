package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: Flink12_SQL_ProcessTime
 * Package: com.atguigu.day08
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/15 0:56
 * @Version 1.0
 */
public class Flink12_SQL_ProcessTime {
    public static void main(String[] args) {

        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.创建表的环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.1在创建表时,并指定处理时间
        tableEnv.executeSql("create table sensor(id string, ts bigint, vc int, pt as PROCTIME()) with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor_sql.txt',"
                + "'format' = 'csv'"
                + ")"
        );

        //3.查询表
        tableEnv.executeSql("select * from sensor").print();

    }
}

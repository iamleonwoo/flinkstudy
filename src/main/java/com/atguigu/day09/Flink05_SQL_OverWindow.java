package com.atguigu.day09;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: Flink05_SQL_OverWindow
 * Package: com.atguigu.day09
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/15 23:26
 * @Version 1.0
 */
public class Flink05_SQL_OverWindow {
    public static void main(String[] args) {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.创建表的环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将默认时区从格林威治时区改为东八区
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.local-time-zone", "GMT");

        //TODO 2.1在创建表时,并指定事件时间
        tableEnv.executeSql("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int, " +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second)" +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor_sql.txt',"
                + "'format' = 'csv'"
                + ")"
        );

        //3.查询表,并使用开窗函数 方式一（先定义再使用）
        tableEnv.executeSql(
                "select " +
                        "id, " +
                        "ts, " +
                        "sum(vc) over w, " +
                        "count(vc) over w " +
                        "from sensor " +
                        "window w as (partition by id order by t)"
        ).print();

        //3.方式二：直接在select中使用
        tableEnv.executeSql(
                "select " +
                        "id, " +
                        "ts, " +
                        "sum(vc) over (partition by id order by t) " +
                        "from sensor"
        ).print();

    }
}

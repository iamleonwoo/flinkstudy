package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * ClassName: Flink10_HiveCatalog
 * Package: com.atguigu.day09
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/16 1:21
 * @Version 1.0
 */
public class Flink10_HiveCatalog {
    public static void main(String[] args) {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 3.创建HiveCataLog
        String hiveCataLogName = "hiveCataLog";
        String database = "flink_test";
        String hiveConf = "D:\\Program Files (x86)\\Hive\\conf";

        HiveCatalog hiveCatalog = new HiveCatalog(hiveCataLogName, database, hiveConf);

        //TODO 4.注册HiveCataLog
        tableEnv.registerCatalog(hiveCataLogName, hiveCatalog);

        //TODO 5.设置使用哪个CataLog，以及哪个数据库
        tableEnv.useCatalog(hiveCataLogName);
        tableEnv.useDatabase(database);

        //指定SQL语法为Hive语法
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnv.executeSql("select * from stu").print();

    }
}

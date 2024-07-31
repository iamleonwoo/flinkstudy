package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * ClassName: Flink05_TableAPI_Connect_Kafka
 * Package: com.atguigu.day08
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/14 23:04
 * @Version 1.0
 */
public class Flink05_TableAPI_Connect_Kafka {
    public static void main(String[] args) {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.创建表的环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        //TODO 3.连接Kafka系统，获取Kafka中的数据
        tableEnv.connect(new Kafka()
                        .version("universal")
                        .topic("sensor")
                        .property("group.id", "bigdata2024")
                        .startFromLatest()
                        .property("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
                ).withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //4.TableAPI将临时表转为Table对象，为了调用相关算子
        Table table = tableEnv.from("sensor");

        //5.查询表
        Table result = table.groupBy($("id"))
                .aggregate($("id").count().as("cnt"))
                .select($("id"), $("cnt"));

        //通过调用execute这个方法返回一个TableResult类型可以直接打印
        TableResult tableResult = result.execute();

        tableResult.print();

    }
}

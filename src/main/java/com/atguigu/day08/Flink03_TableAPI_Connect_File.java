package com.atguigu.day08;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * ClassName: Flink03_TableAPI_Connect_File
 * Package: com.atguigu.day08
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/14 22:13
 * @Version 1.0
 */
public class Flink03_TableAPI_Connect_File {
    public static void main(String[] args) throws Exception {

        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.创建表的环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        //3.连接文件系统，获取文件中的数据
        tableEnv.connect(new FileSystem().path("input/sensor_sql.txt"))
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .withSchema(schema)
                .createTemporaryTable("sensor");

//        //4.TableAPI将临时表转为Table对象，为了调用相关算子
//        Table inputTable = tableEnv.from("sensor");
//
//        //5.查询表
//        Table resultTable = inputTable.select($("id"), $("ts"), $("vc"));
//
//        //6.将表转为流
//        DataStream<Row> resultDstream = tableEnv.toAppendStream(resultTable, Row.class);
//
//        resultDstream.print();
//
//        env.execute();

        //SQl写法：
        Table table = tableEnv.sqlQuery("select * from sensor");
        //通过调用execute这个方法返回一个TableResult类型可以直接打印
        TableResult tableResult = table.execute();
        tableResult.print();

        //或者直接调用executeSql返回一个TableResult类型
        TableResult tableResult1 = tableEnv.executeSql("select * from sensor");
        tableResult1.print();

    }
}

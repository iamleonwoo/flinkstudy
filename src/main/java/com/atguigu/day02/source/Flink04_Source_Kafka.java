package com.atguigu.day02.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * ClassName: Flink04_Source_Kafka
 * Package: com.atguigu.day02.source
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/7 0:01
 * @Version 1.0
 */
public class Flink04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.Kafka相关配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id", "Flink01_Source_Kafka");
        properties.setProperty("auto.offset.reset", "latest");

        //TODO 3.从Kafka获取数据
        // new FlinkKafkaConsumer<>泛型中，要么不写，要么是String
        DataStreamSource<String> streamSource = env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties));

        streamSource.print();

        //4.执行任务
        env.execute();
    }
}

package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * ClassName: Flink01_Sink_Kafka
 * Package: com.atguigu.day03.sink
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/7 22:24
 * @Version 1.0
 */
public class Flink01_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将从端口读过来的数据线转为WaterSensor，再转为Json
        SingleOutputStreamOperator<String> jsonDStream = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                return JSONObject.toJSONString(waterSensor);
            }
        });

        //TODO 4.KafkaSink
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");

        FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<>(
                "sensor",
                new SimpleStringSchema(),
                properties
        );

        jsonDStream.addSink(flinkKafkaProducer);

        env.execute();

    }
}

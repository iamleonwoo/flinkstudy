package com.atguigu.day03.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * ClassName: Flink04_Sink_Custom
 * Package: com.atguigu.day03.sink
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/8 0:06
 * @Version 1.0
 */
public class Flink04_Sink_Custom {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将从端口读过来的数据线转为WaterSensor
        SingleOutputStreamOperator<WaterSensor> myDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //TODO 4.自定义Sink
        myDStream.addSink(new MySqlSink());

        env.execute();

    }

    private static class MySqlSink extends RichSinkFunction<WaterSensor> {
        private Connection connection;
        private PreparedStatement prepareStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("获取连接。。。");
            //1.获取连接
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "123456");

            //2.语句预执行者
            prepareStatement = connection.prepareStatement("insert into sensor values (?,?,?)");

        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {

            prepareStatement.setString(1, value.getId());
            prepareStatement.setLong(2, value.getTs());
            prepareStatement.setInt(3, value.getVc());

            //真正执行语句
            prepareStatement.execute();
            System.out.println("写入数据中......");

        }

        @Override
        public void close() throws Exception {
            System.out.println("关闭连接。。。");
            //关闭语句预执行者以及jdbc连接
            prepareStatement.close();
            connection.close();
        }
    }
}

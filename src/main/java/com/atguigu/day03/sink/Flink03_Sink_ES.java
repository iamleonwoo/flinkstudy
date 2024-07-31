package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;

/**
 * ClassName: Flink03_Sink_ES
 * Package: com.atguigu.day03.sink
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/7 23:27
 * @Version 1.0
 */
public class Flink03_Sink_ES {
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

        //TODO 4.EsSink
        //创建泛型为HttpHost的一个list集合用来存放ES的地址
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9200));
        httpHosts.add(new HttpHost("hadoop103",9200));
        httpHosts.add(new HttpHost("hadoop104",9200));

        ElasticsearchSink.Builder<WaterSensor> sensorBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<WaterSensor>() {
                    @Override
                    public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {
                        //创建Index请求，可以指定要写入的索引，类型，id
                        IndexRequest indexRequest = new IndexRequest("sensor2024", "_doc", element.getId());
                        //将数据转为json并声明为json，写入ES
                        indexRequest.source(JSONObject.toJSONString(element), XContentType.JSON);

                        indexer.add(indexRequest);
                    }
                }
        );

        //因为读的是无界流，所以需要设置刷写时数据的条数，设置为1,代表来一条往ES中写一条
        //注意：生产中不要这样设置为1
        sensorBuilder.setBulkFlushMaxActions(1);

        ElasticsearchSink<WaterSensor> elasticsearchSink = sensorBuilder.build();
        myDStream.addSink(elasticsearchSink);

        env.execute();

    }
}

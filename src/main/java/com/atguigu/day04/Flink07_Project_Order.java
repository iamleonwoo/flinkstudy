package com.atguigu.day04;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * ClassName: Flink07_Project_Order
 * Package: com.atguigu.day04
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/9 23:03
 * @Version 1.0
 */
public class Flink07_Project_Order {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从不同的数据源获取数据
        //订单数据
        DataStreamSource<String> orderStreamSource = env.readTextFile("input/OrderLog.csv");
        //交易数据
        DataStreamSource<String> receStreamSource = env.readTextFile("input/ReceiptLog.csv");

        //3.分别将两个流的数据转为JavaBean
        SingleOutputStreamOperator<OrderEvent> orderDStream = orderStreamSource.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                OrderEvent orderEvent = new OrderEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
                return orderEvent;
            }
        });

        SingleOutputStreamOperator<TxEvent> receDStream = receStreamSource.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                TxEvent txEvent = new TxEvent(
                        split[0],
                        split[1],
                        Long.parseLong(split[2])
                );
                return txEvent;
            }
        });

        //4.连接两条流
        ConnectedStreams<OrderEvent, TxEvent> orderWithReceDStream = orderDStream.connect(receDStream);

        //5.对相同交易码的数据聚和到一块
        ConnectedStreams<OrderEvent, TxEvent> keyedStream = orderWithReceDStream.keyBy("txId", "txId");

        //6.对两个流的数据进行对比
        keyedStream.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
            //缓存订单数据的Map集合
            HashMap<String, OrderEvent> orderMap = new HashMap<>();

            //缓存交易数据的Map集合
            HashMap<String, TxEvent> txEventMap = new HashMap<>();

            @Override
            public void processElement1(OrderEvent value, KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>.Context ctx, Collector<String> out) throws Exception {
                //1.判断对方缓存中是否有能够关联上的数据
                if (txEventMap.containsKey(value.getTxId())) {
                    System.out.println("订单" + value.getOrderId() + "对账成功");
                    //对账成功之后删除对方这个缓存
                    txEventMap.remove(value.getTxId());
                } else {
                    //没有对账成功,把自己存入缓存
                    orderMap.put(value.getTxId(), value);
                }
            }

            @Override
            public void processElement2(TxEvent value, KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>.Context ctx, Collector<String> out) throws Exception {
                //1.判断对方缓存中是否有能够关联上的数据
                if (orderMap.containsKey(value.getTxId())) {
                    System.out.println("订单" + orderMap.get(value.getTxId()).getOrderId() + "对账成功");
                    //对账成功之后删除对方这个缓存
                    orderMap.remove(value.getTxId());
                } else {
                    //没有对账成功,把自己存入缓存
                    txEventMap.put(value.getTxId(), value);
                }
            }
        });

        env.execute();

    }
}

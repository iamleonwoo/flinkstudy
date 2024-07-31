package com.atguigu.day06;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ClassName: Flink07_State_OperatorState_Broadcast
 * Package: com.atguigu.day06
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/12 1:36
 * @Version 1.0
 */
public class Flink07_State_OperatorState_Broadcast {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //获取两条流
        DataStreamSource<String> localStream = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> hadoopStream = env.socketTextStream("hadoop102", 9999);

        //广播一条流
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("broadcast", String.class, String.class);
        BroadcastStream<String> broadcastStream = hadoopStream.broadcast(mapStateDescriptor);

        //连接两条流
        BroadcastConnectedStream<String, String> connectedStream = localStream.connect(broadcastStream);

        //通过一条流的数据来决定另一个流的逻辑
        connectedStream.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, BroadcastProcessFunction<String, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                //获取状态
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                if("1".equals(broadcastState.get("switch"))){
                    out.collect("执行逻辑1。。。");
                } else if ("2".equals(broadcastState.get("switch"))) {
                    out.collect("执行逻辑2。。。");
                } else {
                    out.collect("执行其他逻辑。。。");
                }
            }

            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                //获取广播状态
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                //存放状态
                broadcastState.put("switch",value);
            }
        }).print();

        env.execute();

    }
}

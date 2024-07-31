package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ClassName: Flink01_State_KeyedState_ValueState
 * Package: com.atguigu.day06
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/11 23:22
 * @Version 1.0
 */
public class Flink01_State_KeyedState_ValueState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将读过来的数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //4.因为要使用键控状态，所以要进行keyBy操作
        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(WaterSensor::getId);

        //5.检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            //TODO a.定义一个状态，用来保存上一次的水位值
            private ValueState<Integer> lastVc;

            @Override
            public void open(Configuration parameters) throws Exception {
                //TODO b.初始化状态
                lastVc = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state" ,Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                //TODO c.从状态汇总获取保存的数据
                int lastVcState = lastVc.value() == null ? 0 : lastVc.value();

                if (Math.abs(lastVcState - value.getVc()) > 10) {
                    out.collect("警报！水位线差值超过10~ ");
                }
                //TODO d.将当前的数据保存到状态中
                lastVc.update(value.getVc());
            }
        }).print();

        env.execute();

    }
}

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
import org.apache.flink.util.OutputTag;

/**
 * ClassName: Flink06_Timer_Exe_WithKeyedState
 * Package: com.atguigu.day05
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/12 1:18
 * @Version 1.0
 */
public class Flink06_Timer_Exe_WithKeyedState {
    public static void main(String[] args) throws Exception {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.对数据进行处理，封装成WaterSensor
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //4.将相同元素的数据聚和到一块
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDStream.keyBy(WaterSensor::getId);

        //监控水位传感器的水位值，如果水位值在五秒钟之内连续上升，则报警，并将报警信息输出到侧输出流。
        SingleOutputStreamOperator<WaterSensor> process = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            //声明一个变量用来保存上次水位值
            private ValueState<Integer> lastVc;

            //声明一个变量用来保存定时器时间
            private ValueState<Long> timer;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVc = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVc", Integer.class, Integer.MIN_VALUE));
                timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class, Long.MIN_VALUE));
            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                //1.判断当前水位值是否高于上次水位值
                if (value.getVc() > lastVc.value()) {
                    //如果没有注册定时器那么注册一个定时器
                    if (timer.value() == Long.MIN_VALUE) {
                        System.out.println("注册定时器");
                        //给保存定时器时间的变量重新赋值，以此证明有定时器被注册
                        timer.update(ctx.timerService().currentProcessingTime() + 5000);
                        ctx.timerService().registerProcessingTimeTimer(timer.value());
                    }
                } else {
                    //水位没有上升则重置原来的定时器
                    System.out.println("删除定时器");
                    ctx.timerService().deleteProcessingTimeTimer(timer.value());
                    timer.clear();
                }
                //将当前的水位保存到变量中以便下次来做对比
                lastVc.update(value.getVc());
                out.collect(value);
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, WaterSensor>.OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                //将报警信息输出到侧输出流
                ctx.output(new OutputTag<String>("Warning"){}, "报警！！！！连续5S水位上升" + ctx.getCurrentKey());
                //重置定时器
                timer.clear();
            }
        });

        process.print("主流：");
        process.getSideOutput(new OutputTag<String>("Warning"){}).print("侧流：");

        env.execute();
    }
}

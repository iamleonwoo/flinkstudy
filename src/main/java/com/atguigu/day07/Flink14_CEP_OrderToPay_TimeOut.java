package com.atguigu.day07;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * ClassName: Flink14_CEP_OrderToPay_TimeOut
 * Package: com.atguigu.day07
 * Description: 统计创建订单到下单中间超过15分钟的超时数据以及正常的数据。
 *
 * @Author LeonWoo
 * @Create 2024/4/13 22:36
 * @Version 1.0
 */
public class Flink14_CEP_OrderToPay_TimeOut {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件中获取数据 -> 转为JavaBean -> 生成WaterMark，指定时间字段 -> 按照订单id进行keyBy
        KeyedStream<OrderEvent, Long> orderEventLongKeyedStream = env.readTextFile("input/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new OrderEvent(
                                Long.parseLong(split[0]),
                                split[1],
                                split[2],
                                Long.parseLong(split[3])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                                    @Override
                                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                        return element.getEventTime() * 1000;
                                    }
                                }))
                .keyBy(OrderEvent::getOrderId);

        //统计创建订单到下单中间超过15分钟的超时数据以及正常的数据。
        //TODO 3.定义模式
        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("create")
                //先匹配创建订单形式的事件
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                //再匹配到支付的事件
                .next("pay")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        //TODO 4.将模式作用于流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventLongKeyedStream, pattern);

        //TODO 5.匹配上的数据输出到主流，超时的输出到侧输出流
        SingleOutputStreamOperator<String> result = patternStream.select(
                new OutputTag<String>("超时") {
                },
                new PatternTimeoutFunction<OrderEvent, String>() {
                    @Override
                    public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                        return pattern.toString();
                    }
                },
                new PatternSelectFunction<OrderEvent, String>() {
                    @Override
                    public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                        return pattern.toString();
                    }
                }
        );

        result.print("正常数据");
        result.getSideOutput(new OutputTag<String>("超时"){}).print("超时数据");

        env.execute();

    }
}

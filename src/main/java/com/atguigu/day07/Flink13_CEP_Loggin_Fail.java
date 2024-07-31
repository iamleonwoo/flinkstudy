package com.atguigu.day07;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * ClassName: Flink13_CEP_Loggin_Fail
 * Package: com.atguigu.day07
 * Description: 用户2秒内连续两次及以上登录失败则判定为恶意登录。
 *
 * @Author LeonWoo
 * @Create 2024/4/13 21:43
 * @Version 1.0
 */
public class Flink13_CEP_Loggin_Fail {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件中读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/LoginLog.csv");

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<LoginEvent> loginEventDstream = streamSource.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new LoginEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
            }
        });

        //4.设置watermark
        SingleOutputStreamOperator<LoginEvent> streamOperator = loginEventDstream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.getEventTime() * 1000;
                            }
                        })
        );

        //5.对用户id进行keyBy操作
        KeyedStream<LoginEvent, Long> keyedStream = streamOperator.keyBy(LoginEvent::getUserId);

        //需求：用户2秒内连续两次及以上登录失败则判定为恶意登录
        //TODO 6.定义模式
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("begin")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                //两次及以上
                .timesOrMore(2)
                //连续 -> 严格连续
                .consecutive()
                //用户两秒内 -> 超时时间
                .within(Time.seconds(2));

        //TODO 7.将模式作用于流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 8.将符合规则的数据输出到控制台
        SingleOutputStreamOperator<String> result = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                return pattern.toString();
            }
        });

        result.print();

        env.execute();

    }
}

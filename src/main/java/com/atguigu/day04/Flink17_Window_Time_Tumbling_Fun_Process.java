package com.atguigu.day04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * ClassName: Flink17_Window_Time_Tumbling_Fun_Process
 * Package: com.atguigu.day04
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/10 2:41
 * @Version 1.0
 */
public class Flink17_Window_Time_Tumbling_Fun_Process {
    public static void main(String[] args) throws Exception {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.对数据进行处理，封装成Tuple元组
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDStream = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                out.collect(Tuple2.of(value, 1L));

            }
        });

        //4.将相同元素的数据聚和到一块
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordToOneDStream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });

        // 5.开启基于时间滚动窗口 ->窗口大小为5S
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));


        //TODO 全窗口函数，没有上下文环境
//        window.apply(new WindowFunction<Tuple2<String, Long>, Long, String, TimeWindow>() {
//            @Override
//            public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Long> out) throws Exception {
//
//            }
//        })

        //TODO 6.使用全窗口函数，显示单词累加的功能
        window.process(new ProcessWindowFunction<Tuple2<String, Long>, Long, String, TimeWindow>() {
            //自定义一个累加器
            Long count = 0L;

            @Override
            public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, Long, String, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> elements, Collector<Long> out) throws Exception {
                System.out.println("process...");

                for (Tuple2<String, Long> element : elements) {
                    count = count + element.f1;
                }

                out.collect(count);

            }
        }).print();

        env.execute();

    }
}

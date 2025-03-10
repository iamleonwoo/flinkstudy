package com.atguigu.day04;

import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * ClassName: Flink04_Project_AppAnalysis_By_Chanel
 * Package: com.atguigu.day04
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/9 22:26
 * @Version 1.0
 */
public class Flink04_Project_AppAnalysis_By_Chanel {
    public static void main(String[] args) throws Exception {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<MarketingUserBehavior> streamSource = env.addSource(new AppMarketingDataSource());

        //3.处理数据
        SingleOutputStreamOperator<Tuple2<String, Long>> map = streamSource.map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(MarketingUserBehavior value) throws Exception {
                return Tuple2.of(value.getChannel()+ "-" + value.getBehavior(), 1L);
            }
        });

        //4.将数据聚合起来
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = map.keyBy(0);

        //5.累加
        keyedStream.sum(1).print();

        env.execute();

    }

    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());
                ctx.collect(marketingUserBehavior);
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }

}

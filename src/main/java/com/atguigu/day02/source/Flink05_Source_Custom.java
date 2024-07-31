package com.atguigu.day02.source;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * ClassName: Flink05_Source_Custom
 * Package: com.atguigu.day02.source
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/7 0:29
 * @Version 1.0
 */
public class Flink05_Source_Custom {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.通过自定义数据源获取数据
        DataStreamSource<WaterSensor> streamSource = env.addSource(new MySource());

        streamSource.print();

        //3.执行任务
        env.execute();

    }

    public static class MySource implements SourceFunction<WaterSensor> {

        private Random random = new Random();

        //定义标志位，用来控制是否生成数据
        private boolean running = true;

        /**
         * 发送数据的方法
         * @param ctx The context to emit elements to and for accessing locks.
         * @throws Exception
         */
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (running){
                ctx.collect(new WaterSensor("sensor" + random.nextInt(10),System.currentTimeMillis(),random.nextInt(10)*100));
                Thread.sleep(1000);
            }
        }

        /**
         * 取消任务，一般不自己调用
         */
        @Override
        public void cancel() {
            running = false;
        }
    }

}

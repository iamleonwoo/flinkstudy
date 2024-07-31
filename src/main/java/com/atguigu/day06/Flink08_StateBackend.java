package com.atguigu.day06;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * ClassName: Flink08_StateBackend
 * Package: com.atguigu.day06
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/12 2:11
 * @Version 1.0
 */
public class Flink08_StateBackend {
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 基于内存的
        env.setStateBackend(new MemoryStateBackend());

        //TODO 基于文件系统的
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));

        //TODO 基于RocksDS的
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flink/rocksDb"));

    }
}

package org.apache.flink.table.examples.java.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkUtils {


    public static void initEnv(StreamExecutionEnvironment env, ParameterTool parameterTool){
        //checkpoint
        env.getConfig().setGlobalJobParameters((ExecutionConfig.GlobalJobParameters)parameterTool);
        env.enableCheckpointing(parameterTool.getLong("checkpointInterval", 1*60*1000L));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);
        env.getCheckpointConfig().setCheckpointTimeout(30*1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //失败重试次数（最大重试次数，时间区间，重试间隔）
        //env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.seconds(60), Time.seconds(10)));

        //如果flink-conf.yaml 中统一配置，代码中可以不配置
        /*FsStateBackend stateBackend = new FsStateBackend("hdfs://192.168.100.20:9000/flink/chk01");
        env.setStateBackend((StateBackend)stateBackend);*/

        //并行度设置
        //env.setParallelism(parameterTool.getInt("p", 1));
    }

}

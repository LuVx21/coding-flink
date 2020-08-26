package org.luvx.async;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName: org.luvx.async
 * @Description:
 * @Author: Ren, Xie
 */
public class AsyncMain {
    private static final String EXACTLY_ONCE_MODE = "exactly_once";
    private static final String EVENT_TIME        = "EventTime";
    private static final String INGESTION_TIME    = "IngestionTime";
    private static final String ORDERED           = "ordered";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);

        final String statePath;
        final String cpMode;
        final int maxCount;
        final long sleepFactor;
        final float failRatio;
        final String mode;
        final int taskNum;
        final String timeType;
        final long shutdownWaitTS;
        final long timeout;

        try {
            statePath = params.get("fsStatePath", null);
            cpMode = params.get("checkpointMode", "exactly_once");
            maxCount = params.getInt("maxCount", 100000);
            sleepFactor = params.getLong("sleepFactor", 100);
            failRatio = params.getFloat("failRatio", 0.001f);
            mode = params.get("waitMode", "ordered");
            taskNum = params.getInt("waitOperatorParallelism", 1);
            timeType = params.get("eventType", "EventTime");
            shutdownWaitTS = params.getLong("shutdownWaitTS", 20000);
            timeout = params.getLong("timeout", 10000L);
        } catch (Exception e) {
            throw e;
        }

        if (statePath != null) {
            env.setStateBackend(new FsStateBackend(statePath));
        }

        if (EXACTLY_ONCE_MODE.equals(cpMode)) {
            env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        } else {
            env.enableCheckpointing(1000L, CheckpointingMode.AT_LEAST_ONCE);
        }

        if (EVENT_TIME.equals(timeType)) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        } else if (INGESTION_TIME.equals(timeType)) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        }

        DataStream<Integer> inputStream = env.addSource(new SimpleSource(maxCount));
        AsyncFunction<Integer, String> function = new SampleAsyncFunction(sleepFactor, failRatio, shutdownWaitTS);

        DataStream<String> result;
        if (ORDERED.equals(mode)) {
            result = AsyncDataStream.orderedWait(inputStream, function, timeout, TimeUnit.MILLISECONDS,
                    20).setParallelism(taskNum);
        } else {
            result = AsyncDataStream.unorderedWait(inputStream, function, timeout, TimeUnit.MILLISECONDS,
                    20).setParallelism(taskNum);
        }

        result.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(new Tuple2<>(value, 1));
                    }
                }
        )
                .keyBy(0)
                .sum(1)
                .print("async");

        env.execute("Async IO Example");
    }
}

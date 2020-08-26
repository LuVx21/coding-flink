package org.luvx.common.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName: org.luvx.utils
 * @Description:
 * @Author: Ren, Xie
 */
@Slf4j
public class CheckPointUtil {
    public static final String STREAM_CHECKPOINT_ENABLE   = "stream.checkpoint.enable";
    public static final String STREAM_CHECKPOINT_DIR      = "stream.checkpoint.dir";
    public static final String STREAM_CHECKPOINT_TYPE     = "stream.checkpoint.type";
    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";
    public static final String CHECKPOINT_MEMORY          = "memory";
    public static final String CHECKPOINT_FS              = "fs";
    public static final String CHECKPOINT_ROCKETSDB       = "rocksdb";

    private static final String path_windows = "file:///D:/data/flink/checkpoint1";
    private static final String path_linux   = "/data/flink/checkpoint";

    public static StreamExecutionEnvironment setCheckpointConfig(StreamExecutionEnvironment env) throws URISyntaxException {
        env.enableCheckpointing(30_000);
        StateBackend stateBackend = new FsStateBackend(new URI(path_windows), 0);
        env.setStateBackend(stateBackend);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 模式
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 任务取消时, 记录point
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 最小间隔 500 ms
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // 设置point超时时间, 否则丢弃
        checkpointConfig.setCheckpointTimeout(60_000);
        // 设置point失败时，任务不会 fail, 丢弃point
        /// checkpointConfig.setFailOnCheckpointingErrors(false); // 1.9.1 废弃
        checkpointConfig.setTolerableCheckpointFailureNumber(0);
        // 并发度为 1
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        return env;
    }
}

package org.luvx.async;

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Collections;
import java.util.List;

/**
 * @ClassName: org.luvx.async
 * @Description:
 * @Author: Ren, Xie
 */
public class SimpleSource implements SourceFunction<Integer>, ListCheckpointed<Integer> {
    private static final long serialVersionUID = 1L;

    private volatile boolean isRunning = true;
    private          int     counter   = 0;
    private          int     start     = 0;

    public SimpleSource(int maxNum) {
        this.counter = maxNum;
    }

    @Override
    public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
        return Collections.singletonList(start);
    }

    @Override
    public void restoreState(List<Integer> state) throws Exception {
        for (Integer i : state) {
            this.start = i;
        }
    }

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        while ((start < counter || counter == -1) && isRunning) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(start);
                ++start;

                if (start == Integer.MAX_VALUE) {
                    start = 0;
                }
            }
            Thread.sleep(10L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

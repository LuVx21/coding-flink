package org.luvx.async;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.ExecutorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: org.luvx.async
 * @Description:
 * @Author: Ren, Xie
 */
public class SampleAsyncFunction extends RichAsyncFunction<Integer, String> {
    private static final long serialVersionUID = 2098635244857937717L;

    private final     long            sleepFactor;
    private final     float           failRatio;
    private final     long            shutdownWaitTS;
    private transient ExecutorService executorService;

    SampleAsyncFunction(long sleepFactor, float failRatio, long shutdownWaitTS) {
        this.sleepFactor = sleepFactor;
        this.failRatio = failRatio;
        this.shutdownWaitTS = shutdownWaitTS;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        executorService = Executors.newFixedThreadPool(30);
    }

    @Override
    public void asyncInvoke(final Integer input, final ResultFuture<String> resultFuture) {
        executorService.submit(() -> {
            long sleep = (long) (ThreadLocalRandom.current().nextFloat() * sleepFactor);
            try {
                Thread.sleep(sleep);

                if (ThreadLocalRandom.current().nextFloat() < failRatio) {
                    resultFuture.completeExceptionally(new Exception("wahahahaha..."));
                } else {
                    resultFuture.complete(
                            Collections.singletonList("key-" + (input % 10)));
                }
            } catch (InterruptedException e) {
                resultFuture.complete(new ArrayList<>(0));
            }
        });
    }

    @Override
    public void close() throws Exception {
        super.close();
        ExecutorUtils.gracefulShutdown(shutdownWaitTS, TimeUnit.MILLISECONDS, executorService);
    }
}

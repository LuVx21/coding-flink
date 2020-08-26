package org.luvx.common;

import java.io.Serializable;
import java.util.Iterator;

import static java.util.Objects.requireNonNull;

/**
 * @ClassName: org.luvx.join
 * @Description: 迭代器 用于造数据
 * @Author: Ren, Xie
 */
public class ThrottledIterator<T> implements Iterator<T>, Serializable {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("NonSerializableFieldInSerializableClass")
    private final Iterator<T> source;
    private final long        sleepBatchSize;
    private final long        sleepBatchTime;
    private       long        lastBatchCheckTime;
    private       long        num;

    public ThrottledIterator(Iterator<T> source, long elementsPerSecond) {
        this.source = requireNonNull(source, "数据源不可为空");

        if (!(source instanceof Serializable)) {
            throw new IllegalArgumentException("source must be java.io.Serializable");
        }

        if (elementsPerSecond >= 100) {
            this.sleepBatchSize = elementsPerSecond / 20;
            this.sleepBatchTime = 50;
        } else if (elementsPerSecond >= 1) {
            this.sleepBatchSize = 1;
            this.sleepBatchTime = 1000 / elementsPerSecond;
        } else {
            throw new IllegalArgumentException("'elements per second' must be positive and not zero");
        }
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public T next() {
        if (lastBatchCheckTime > 0) {
            if (++num >= sleepBatchSize) {
                num = 0;
                final long now = System.currentTimeMillis();
                final long elapsed = now - lastBatchCheckTime;
                if (elapsed < sleepBatchTime) {
                    try {
                        Thread.sleep(sleepBatchTime - elapsed);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                lastBatchCheckTime = now;
            }
        } else {
            lastBatchCheckTime = System.currentTimeMillis();
        }

        return source.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
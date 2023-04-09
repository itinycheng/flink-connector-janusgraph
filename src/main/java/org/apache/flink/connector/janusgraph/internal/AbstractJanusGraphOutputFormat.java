package org.apache.flink.connector.janusgraph.internal;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.io.Flushable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Abstract output format. */
public abstract class AbstractJanusGraphOutputFormat<T> extends RichOutputFormat<T>
        implements Flushable, InputTypeConfigurable {

    private TypeSerializer<T> serializer;

    protected transient volatile boolean closed = false;

    protected transient ScheduledExecutorService scheduler;

    protected transient ScheduledFuture<?> scheduledFuture;

    private transient volatile Exception flushException;

    @Override
    public void configure(Configuration parameters) {}

    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        if (executionConfig.isObjectReuseEnabled()) {
            this.serializer = (TypeSerializer<T>) type.createSerializer(executionConfig);
        }
    }

    @Override
    public void close() {
        closed = true;
        if (this.scheduledFuture != null) {
            scheduledFuture.cancel(false);
            this.scheduler.shutdown();
        }
    }

    protected void scheduledFlush(long intervalMillis, String executorName) {
        Preconditions.checkArgument(intervalMillis > 0, "flush interval must be greater than 0");
        scheduler = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory(executorName));
        scheduledFuture =
                scheduler.scheduleWithFixedDelay(
                        () -> {
                            synchronized (this) {
                                if (!closed) {
                                    try {
                                        flush();
                                    } catch (Exception e) {
                                        flushException = e;
                                    }
                                }
                            }
                        },
                        intervalMillis,
                        intervalMillis,
                        TimeUnit.MILLISECONDS);
    }

    protected T copyIfNecessary(T record) {
        return serializer == null ? record : serializer.copy(record);
    }

    protected void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to JanusGraph failed.", flushException);
        }
    }
}

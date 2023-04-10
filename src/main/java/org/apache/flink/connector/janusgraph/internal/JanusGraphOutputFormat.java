package org.apache.flink.connector.janusgraph.internal;

import org.apache.flink.connector.janusgraph.internal.connection.JanusGraphConnectionProvider;
import org.apache.flink.connector.janusgraph.internal.executor.JanusGraphExecutor;
import org.apache.flink.connector.janusgraph.options.JanusGraphOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default output format. */
public class JanusGraphOutputFormat<T> extends AbstractJanusGraphOutputFormat<T> {

    private static final Logger LOG = LoggerFactory.getLogger(JanusGraphExecutor.class);

    private final JanusGraphConnectionProvider connectionProvider;

    private final String[] fieldNames;

    private final String[] keyFields;

    private final LogicalType[] fieldTypes;

    private final Function<T, RowData> valueTransformer;

    private final JanusGraphOptions options;

    private transient JanusGraphExecutor executor;

    private transient int batchCount = 0;

    protected JanusGraphOutputFormat(
            @Nonnull JanusGraphConnectionProvider connectionProvider,
            @Nonnull String[] keyFields,
            @Nonnull String[] fieldNames,
            @Nonnull LogicalType[] fieldTypes,
            @Nonnull Function<T, RowData> valueTransformer,
            @Nonnull JanusGraphOptions options) {
        this.connectionProvider = checkNotNull(connectionProvider);
        this.keyFields = checkNotNull(keyFields);
        this.fieldNames = checkNotNull(fieldNames);
        this.valueTransformer = checkNotNull(valueTransformer);
        this.fieldTypes = checkNotNull(fieldTypes);
        this.options = checkNotNull(options);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            executor = JanusGraphExecutor.createExecutor(fieldNames, fieldTypes, options);
            executor.setRuntimeContext(getRuntimeContext());
            executor.prepareBatch(connectionProvider);

            long flushIntervalMillis = options.getFlushInterval().toMillis();
            scheduledFlush(flushIntervalMillis, "janusgraph-output-format");
        } catch (Exception e) {
            throw new IOException("Unable to establish connection with JanusGraph.", e);
        }
    }

    @Override
    public synchronized void writeRecord(T record) throws IOException {
        checkFlushException();

        try {
            T recordCopy = copyIfNecessary(record);
            RowData rowData = valueTransformer.apply(recordCopy);
            executor.addToBatch(rowData);
            batchCount++;
            if (options.getBatchSize() > 0 && batchCount >= options.getBatchSize()) {
                flush();
            }
        } catch (Exception e) {
            throw new IOException("Writing records to JanusGraph failed.", e);
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        checkFlushException();
        int maxRetries = options.getMaxRetries();
        for (int i = 0; i <= maxRetries; i++) {
            try {
                executor.executeBatch();
                batchCount = 0;
                return;
            } catch (Exception e) {
                LOG.error("JanusGraph commitBatch error, retry times = {}", i, e);
                if (i >= maxRetries) {
                    throw new RuntimeException(
                            String.format(
                                    "Attempt to commit batch failed, exhausted retry times = %d",
                                    maxRetries),
                            e);
                }
                try {
                    Thread.sleep(1000L * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "Unable to flush; interrupted while doing another attempt", ex);
                }
            }
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }

        super.close();

        if (batchCount > 0) {
            try {
                flush();
            } catch (Exception e) {
                LOG.warn("Writing records to JanusGraph failed.", e);
                throw new RuntimeException("Writing records to JanusGraph failed.", e);
            }
        }

        try {
            executor.close();
        } catch (Exception e) {
            LOG.warn("Close JanusGraph executor failed.", e);
        }

        try {
            connectionProvider.close();
        } catch (Exception e) {
            LOG.warn("Close JanusGraph connection failed.", e);
        }

        checkFlushException();
    }
}

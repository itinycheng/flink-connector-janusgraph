package org.apache.flink.connector.janusgraph.internal.executor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.janusgraph.internal.connection.JanusGraphConnectionProvider;
import org.apache.flink.connector.janusgraph.options.JanusGraphOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** JanusGraph vertex upsert executor. */
public class JanusGraphVertexUpsertExecutor extends JanusGraphExecutor {

    private final JanusGraphVertexExecutor executor;

    public JanusGraphVertexUpsertExecutor(
            @Nonnull JanusGraphVertexExecutor executor, @Nonnull JanusGraphOptions options) {
        super(options);
        this.executor = checkNotNull(executor);
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {
        executor.setRuntimeContext(context);
    }

    @Override
    public void prepareBatch(JanusGraphConnectionProvider connectionProvider) {
        executor.prepareBatch(connectionProvider);
    }

    @Override
    public void addToBatch(RowData record) {
        if (RowKind.INSERT == record.getRowKind()) {
            record.setRowKind(RowKind.UPDATE_AFTER);
        }

        executor.addToBatch(record);
    }

    @Override
    public void executeBatch() {
        executor.executeBatch();
    }

    @Override
    public void close() {
        executor.close();
    }
}

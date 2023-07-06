package org.apache.flink.connector.janusgraph.internal.executor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.janusgraph.internal.connection.JanusGraphConnectionProvider;
import org.apache.flink.connector.janusgraph.options.JanusGraphOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.janusgraph.core.JanusGraphTransaction;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** JanusGraph edge upsert executor. */
public class JanusGraphEdgeUpsertExecutor extends JanusGraphExecutor {

    private final JanusGraphEdgeExecutor executor;

    public JanusGraphEdgeUpsertExecutor(
            @Nonnull JanusGraphEdgeExecutor executor, @Nonnull JanusGraphOptions options) {
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
    public void addToBatch(RowData rowData) {
        if (RowKind.INSERT == rowData.getRowKind()) {
            rowData.setRowKind(RowKind.UPDATE_AFTER);
        }

        executor.addToBatch(rowData);
    }

    @Override
    public void executeBatch() {
        executor.executeBatch();
    }

    @Override
    protected void execute(RowData record, JanusGraphTransaction transaction) {
        executor.execute(record, transaction);
    }

    @Override
    public void close() {
        executor.close();
    }
}

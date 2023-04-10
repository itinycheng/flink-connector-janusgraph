package org.apache.flink.connector.janusgraph.internal.executor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.janusgraph.config.TableType;
import org.apache.flink.connector.janusgraph.internal.connection.JanusGraphConnection;
import org.apache.flink.connector.janusgraph.internal.connection.JanusGraphConnectionProvider;
import org.apache.flink.connector.janusgraph.internal.converter.JanusGraphRowConverter;
import org.apache.flink.connector.janusgraph.internal.helper.VertexByIdSearcher;
import org.apache.flink.connector.janusgraph.internal.helper.VertexByPropSearcher;
import org.apache.flink.connector.janusgraph.internal.helper.VertexObjectSearcher;
import org.apache.flink.connector.janusgraph.options.JanusGraphOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.ArrayUtils;
import org.janusgraph.core.JanusGraphTransaction;

import java.io.Serializable;

import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_FROM_V_ID;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_LABEL;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_TO_V_ID;
import static org.apache.flink.connector.janusgraph.config.TableType.EDGE;
import static org.apache.flink.connector.janusgraph.config.TableType.VERTEX;

/** Executor interface for submitting data to ClickHouse. */
public abstract class JanusGraphExecutor implements Serializable {

    protected RuntimeContext runtimeContext;

    protected int maxRetries;

    protected transient JanusGraphConnection connection;

    protected transient JanusGraphTransaction transaction;

    public void setRuntimeContext(RuntimeContext context) {
        this.runtimeContext = context;
    }

    public void prepareBatch(JanusGraphConnectionProvider connectionProvider) {
        this.connection = connectionProvider.getOrCreateConnection();
        this.transaction = connection.newTransaction();
    }

    public abstract void addToBatch(RowData rowData) throws Exception;

    public void executeBatch() {
        transaction.commit();
        transaction.close();
        transaction = connection.newTransaction();
    }

    public void close() {
        if (transaction != null && transaction.isOpen()) {
            try {
                transaction.commit();
            } finally {
                transaction.close();
            }
        }

        if (connection != null) {
            connection.close();
        }

        transaction = null;
        connection = null;
    }

    public static JanusGraphExecutor createExecutor(
            String[] fieldNames, LogicalType[] fieldTypes, JanusGraphOptions options) {
        TableType tableType = options.getTableType();
        if (tableType == EDGE) {
            return createEdgeExecutor(fieldNames, fieldTypes, options);
        } else if (tableType == VERTEX) {
            return createVertexExecutor(fieldNames, fieldTypes, options);
        } else {
            throw new RuntimeException("Unknown table type: " + tableType);
        }
    }

    static JanusGraphVertexExecutor createVertexExecutor(
            String[] fieldNames, LogicalType[] fieldTypes, JanusGraphOptions options) {
        return new JanusGraphVertexExecutor(
                fieldNames, new JanusGraphRowConverter(RowType.of(fieldTypes)), options);
    }

    static JanusGraphEdgeExecutor createEdgeExecutor(
            String[] fieldNames, LogicalType[] fieldTypes, JanusGraphOptions options) {
        return new JanusGraphEdgeExecutor(
                fieldNames,
                ArrayUtils.indexOf(fieldNames, KEYWORD_LABEL),
                createVertexSearcher(fieldNames, fieldTypes, KEYWORD_FROM_V_ID),
                createVertexSearcher(fieldNames, fieldTypes, KEYWORD_TO_V_ID),
                new JanusGraphRowConverter(RowType.of(fieldTypes)),
                options);
    }

    private static VertexObjectSearcher createVertexSearcher(
            String[] fieldNames, LogicalType[] fieldTypes, String vertexColumn) {
        int vertexColumnIndex = ArrayUtils.indexOf(fieldNames, vertexColumn);
        LogicalType vertexColumnType = fieldTypes[vertexColumnIndex];
        if (vertexColumnType.isNullable()) {
            throw new RuntimeException("Vertex searcher cannot accept an nullable data");
        }

        if (LogicalTypeRoot.BIGINT.equals(vertexColumnType.getTypeRoot())) {
            return new VertexByIdSearcher(vertexColumn, vertexColumnType, vertexColumnIndex);
        } else if (LogicalTypeRoot.MAP.equals(vertexColumnType.getTypeRoot())) {
            return new VertexByPropSearcher(vertexColumn, vertexColumnType, vertexColumnIndex);
        } else {
            throw new RuntimeException("Vertex searcher only support Longs and Maps");
        }
    }
}

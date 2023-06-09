package org.apache.flink.connector.janusgraph.internal.executor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.janusgraph.config.TableType;
import org.apache.flink.connector.janusgraph.internal.connection.JanusGraphConnection;
import org.apache.flink.connector.janusgraph.internal.connection.JanusGraphConnectionProvider;
import org.apache.flink.connector.janusgraph.internal.converter.JanusGraphRowConverter;
import org.apache.flink.connector.janusgraph.internal.helper.EdgeByIdSearcher;
import org.apache.flink.connector.janusgraph.internal.helper.EdgeByPropSearcher;
import org.apache.flink.connector.janusgraph.internal.helper.ElementObjectSearcher;
import org.apache.flink.connector.janusgraph.internal.helper.VertexByIdSearcher;
import org.apache.flink.connector.janusgraph.internal.helper.VertexByPropSearcher;
import org.apache.flink.connector.janusgraph.options.JanusGraphOptions;
import org.apache.flink.connector.janusgraph.options.UpdateNotFoundStrategy;
import org.apache.flink.connector.janusgraph.options.WriteMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphTransaction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_ID;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_IN_V;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_LABEL;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_OUT_V;
import static org.apache.flink.connector.janusgraph.config.TableType.EDGE;
import static org.apache.flink.connector.janusgraph.config.TableType.VERTEX;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Executor interface for submitting data to JanusGraph. */
public abstract class JanusGraphExecutor implements Serializable {

    protected RuntimeContext runtimeContext;

    protected final int maxRetries;

    protected final UpdateNotFoundStrategy updateNotFoundStrategy;

    protected final Set<Integer> nonWriteColumnIndexes;

    protected final Set<Integer> nonUpdateColumnIndexes;

    protected transient JanusGraphConnection connection;

    private final List<RowData> buffer = new ArrayList<>();

    public JanusGraphExecutor(JanusGraphOptions options) {
        checkArgument(options != null && options.getMaxRetries() >= 0);
        checkNotNull(options.getUpdateNotFoundStrategy());

        this.maxRetries = options.getMaxRetries();
        this.updateNotFoundStrategy = options.getUpdateNotFoundStrategy();
        this.nonWriteColumnIndexes = new HashSet<>();
        this.nonUpdateColumnIndexes = new HashSet<>();
    }

    public void setRuntimeContext(RuntimeContext context) {
        this.runtimeContext = context;
    }

    public void prepareBatch(JanusGraphConnectionProvider connectionProvider) {
        this.connection = connectionProvider.getOrCreateConnection();
    }

    public void addToBatch(RowData rowData) {
        buffer.add(rowData);
    }

    public void executeBatch() {
        if (buffer.isEmpty()) {
            return;
        }

        JanusGraphTransaction transaction = connection.newTransaction();
        for (RowData value : buffer) {
            execute(value, transaction);
        }

        transaction.commit();
        transaction.close();
        buffer.clear();
    }

    /** transfer rowData to transaction. */
    protected abstract void execute(RowData rowData, JanusGraphTransaction transaction);

    public void close() {
        if (connection != null) {
            connection.close();
        }

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

    static JanusGraphExecutor createVertexExecutor(
            String[] fieldNames, LogicalType[] fieldTypes, JanusGraphOptions options) {
        JanusGraphVertexExecutor executor =
                new JanusGraphVertexExecutor(
                        fieldNames,
                        ArrayUtils.indexOf(fieldNames, KEYWORD_LABEL),
                        createVertexSearcher(fieldNames, fieldTypes, KEYWORD_ID),
                        new JanusGraphRowConverter(RowType.of(fieldTypes)),
                        getNonUpdateColumnIndexes(fieldNames, options.getNonUpdateColumns()),
                        options);
        return options.getMode() == WriteMode.UPSERT
                ? new JanusGraphVertexUpsertExecutor(executor, options)
                : executor;
    }

    static JanusGraphExecutor createEdgeExecutor(
            String[] fieldNames, LogicalType[] fieldTypes, JanusGraphOptions options) {
        JanusGraphEdgeExecutor executor =
                new JanusGraphEdgeExecutor(
                        fieldNames,
                        ArrayUtils.indexOf(fieldNames, KEYWORD_LABEL),
                        createEdgeSearcher(fieldNames, fieldTypes, KEYWORD_ID),
                        createVertexSearcher(fieldNames, fieldTypes, KEYWORD_IN_V),
                        createVertexSearcher(fieldNames, fieldTypes, KEYWORD_OUT_V),
                        new JanusGraphRowConverter(RowType.of(fieldTypes)),
                        getNonUpdateColumnIndexes(fieldNames, options.getNonUpdateColumns()),
                        options);
        return options.getMode() == WriteMode.UPSERT
                ? new JanusGraphEdgeUpsertExecutor(executor, options)
                : executor;
    }

    private static ElementObjectSearcher<Edge> createEdgeSearcher(
            String[] fieldNames, LogicalType[] fieldTypes, String edgeColumn) {
        int edgeColumnIndex = ArrayUtils.indexOf(fieldNames, edgeColumn);
        LogicalType edgeColumnType = fieldTypes[edgeColumnIndex];
        if (LogicalTypeRoot.BIGINT.equals(edgeColumnType.getTypeRoot())) {
            return new EdgeByIdSearcher(edgeColumnType, edgeColumnIndex);
        } else if (LogicalTypeRoot.ROW.equals(edgeColumnType.getTypeRoot())) {
            int inVertexIndex = ArrayUtils.indexOf(fieldNames, KEYWORD_IN_V);
            int outVertexIndex = ArrayUtils.indexOf(fieldNames, KEYWORD_OUT_V);
            return new EdgeByPropSearcher(
                    edgeColumnIndex,
                    ArrayUtils.indexOf(fieldNames, KEYWORD_LABEL),
                    inVertexIndex,
                    outVertexIndex,
                    edgeColumnType,
                    fieldTypes[inVertexIndex],
                    fieldTypes[outVertexIndex]);
        } else {
            throw new RuntimeException("Vertex searcher only support Longs and Maps");
        }
    }

    private static ElementObjectSearcher<Vertex> createVertexSearcher(
            String[] fieldNames, LogicalType[] fieldTypes, String vertexColumn) {
        int vertexColumnIndex = ArrayUtils.indexOf(fieldNames, vertexColumn);
        LogicalType vertexColumnType = fieldTypes[vertexColumnIndex];
        if (LogicalTypeRoot.BIGINT.equals(vertexColumnType.getTypeRoot())) {
            return new VertexByIdSearcher(vertexColumnIndex, vertexColumnType);
        } else if (LogicalTypeRoot.ROW.equals(vertexColumnType.getTypeRoot())) {
            return new VertexByPropSearcher(vertexColumnIndex, vertexColumnType);
        } else {
            throw new RuntimeException("Vertex searcher only support Longs and Maps");
        }
    }

    private static List<Integer> getNonUpdateColumnIndexes(
            String[] fieldNames, String[] nonUpdateColumnNames) {
        List<Integer> nonUpdateColumnIndexes = new ArrayList<>(nonUpdateColumnNames.length);
        for (String nonUpdateColumnName : nonUpdateColumnNames) {
            int nonUpdateColumnIndex = ArrayUtils.indexOf(fieldNames, nonUpdateColumnName);
            if (nonUpdateColumnIndex < 0) {
                throw new RuntimeException(nonUpdateColumnName + " is not a valid column name.");
            }
            nonUpdateColumnIndexes.add(nonUpdateColumnIndex);
        }

        return nonUpdateColumnIndexes;
    }
}

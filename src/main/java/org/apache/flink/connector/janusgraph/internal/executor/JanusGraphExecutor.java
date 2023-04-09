package org.apache.flink.connector.janusgraph.internal.executor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.janusgraph.config.TableType;
import org.apache.flink.connector.janusgraph.internal.connection.JanusGraphConnectionProvider;
import org.apache.flink.connector.janusgraph.internal.converter.JanusGraphRowConverter;
import org.apache.flink.connector.janusgraph.options.JanusGraphOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

import static org.apache.flink.connector.janusgraph.config.TableType.EDGE;
import static org.apache.flink.connector.janusgraph.config.TableType.VERTEX;

/** Executor interface for submitting data to ClickHouse. */
public abstract class JanusGraphExecutor implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(JanusGraphExecutor.class);

    protected RuntimeContext runtimeContext;

    protected int maxRetries;

    public void setRuntimeContext(RuntimeContext context) {
        this.runtimeContext = context;
    }

    public abstract void prepareBatch(JanusGraphConnectionProvider connectionProvider)
            throws IOException;

    public abstract void addToBatch(RowData rowData) throws Exception;

    public abstract void executeBatch() throws Exception;

    public abstract void close();

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
                fieldNames, new JanusGraphRowConverter(RowType.of(fieldTypes)), options);
    }
}

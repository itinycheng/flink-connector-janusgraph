package org.apache.flink.connector.janusgraph.internal.executor;

import org.apache.flink.connector.janusgraph.internal.connection.JanusGraphConnection;
import org.apache.flink.connector.janusgraph.internal.connection.JanusGraphConnectionProvider;
import org.apache.flink.connector.janusgraph.internal.converter.JanusGraphRowConverter;
import org.apache.flink.connector.janusgraph.options.JanusGraphOptions;
import org.apache.flink.table.data.RowData;

import org.apache.tinkerpop.gremlin.structure.T;
import org.janusgraph.core.JanusGraphTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/** JanusGraph edge executor. */
public class JanusGraphEdgeExecutor extends JanusGraphExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(JanusGraphEdgeExecutor.class);

    private static final Map<String, Object> RESERVED_FIELDS;

    private final String[] fieldNames;

    private final JanusGraphRowConverter converter;

    private transient JanusGraphConnection connection;

    private transient JanusGraphTransaction transaction;

    static {
        Map<String, Object> reservedKeywordMap = new HashMap<>();
        reservedKeywordMap.put("id", T.id);
        reservedKeywordMap.put("label", T.label);
        RESERVED_FIELDS = unmodifiableMap(reservedKeywordMap);
    }

    public JanusGraphEdgeExecutor(
            String[] fieldNames, JanusGraphRowConverter converter, JanusGraphOptions options) {
        this.fieldNames = fieldNames;
        this.converter = converter;
        this.maxRetries = options.getMaxRetries();
    }

    @Override
    public void prepareBatch(JanusGraphConnectionProvider connectionProvider) {
        this.connection = connectionProvider.getOrCreateConnection();
        this.transaction = connection.newTransaction();
    }

    @Override
    public void addToBatch(RowData record) {
        switch (record.getRowKind()) {
            case INSERT:
                Object[] keyValuePairs = mergeWithFieldNames(converter.toExternal(record));
                transaction.addVertex(keyValuePairs);
                break;
            case UPDATE_AFTER:
            case DELETE:
            case UPDATE_BEFORE:
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE, but get: %s.",
                                record.getRowKind()));
        }
    }

    private Object[] mergeWithFieldNames(Object[] values) {
        Object[] keyValuePairs = new Object[values.length * 2];
        for (int i = 0; i < values.length; i++) {
            int pos = i * 2;
            String fieldName = fieldNames[i];
            keyValuePairs[pos] = RESERVED_FIELDS.getOrDefault(fieldName, fieldName);
            keyValuePairs[pos + 1] = values[i];
        }
        return keyValuePairs;
    }

    @Override
    public void executeBatch() {
        transaction.commit();
        transaction = connection.newTransaction();
    }

    @Override
    public synchronized void close() {
        if (transaction != null) {
            transaction.close();
        }

        if (connection != null) {
            connection.close();
        }

        transaction = null;
        connection = null;
    }
}

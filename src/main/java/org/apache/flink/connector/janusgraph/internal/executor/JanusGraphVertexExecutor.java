package org.apache.flink.connector.janusgraph.internal.executor;

import org.apache.flink.connector.janusgraph.internal.converter.JanusGraphRowConverter;
import org.apache.flink.connector.janusgraph.options.JanusGraphOptions;
import org.apache.flink.table.data.RowData;

import org.apache.tinkerpop.gremlin.structure.T;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_LABEL;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_V_ID;

/** JanusGraph vertex executor. */
public class JanusGraphVertexExecutor extends JanusGraphExecutor {

    private static final Map<String, Object> RESERVED_FIELDS;

    private final String[] fieldNames;

    private final JanusGraphRowConverter converter;

    static {
        Map<String, Object> reservedKeywordMap = new HashMap<>();
        reservedKeywordMap.put(KEYWORD_V_ID, T.id);
        reservedKeywordMap.put(KEYWORD_LABEL, T.label);
        RESERVED_FIELDS = unmodifiableMap(reservedKeywordMap);
    }

    public JanusGraphVertexExecutor(
            String[] fieldNames, JanusGraphRowConverter converter, JanusGraphOptions options) {
        this.fieldNames = fieldNames;
        this.converter = converter;
        this.maxRetries = options.getMaxRetries();
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
}

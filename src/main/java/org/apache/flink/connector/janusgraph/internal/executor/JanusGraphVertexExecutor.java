package org.apache.flink.connector.janusgraph.internal.executor;

import org.apache.flink.connector.janusgraph.internal.converter.JanusGraphRowConverter;
import org.apache.flink.connector.janusgraph.internal.helper.ElementObjectSearcher;
import org.apache.flink.connector.janusgraph.options.JanusGraphOptions;
import org.apache.flink.table.data.RowData;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_ID;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_LABEL;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** JanusGraph vertex executor. */
public class JanusGraphVertexExecutor extends JanusGraphExecutor {

    private static final Map<String, Object> RESERVED_FIELDS;

    private final String[] fieldNames;

    private final ElementObjectSearcher<Vertex> vertexSearcher;

    private final JanusGraphRowConverter converter;

    private final int vertexKeyValueCount;

    static {
        Map<String, Object> reservedKeywordMap = new HashMap<>();
        reservedKeywordMap.put(KEYWORD_ID, T.id);
        reservedKeywordMap.put(KEYWORD_LABEL, T.label);
        RESERVED_FIELDS = unmodifiableMap(reservedKeywordMap);
    }

    public JanusGraphVertexExecutor(
            @Nonnull String[] fieldNames,
            @Nonnull Integer labelIndex,
            @Nonnull ElementObjectSearcher<Vertex> vertexSearcher,
            @Nonnull JanusGraphRowConverter converter,
            @Nonnull List<Integer> nonUpdateColumnIndexes,
            @Nonnull JanusGraphOptions options) {
        super(options);

        checkArgument(labelIndex >= 0);
        this.fieldNames = checkNotNull(fieldNames);
        this.vertexSearcher = checkNotNull(vertexSearcher);
        this.converter = checkNotNull(converter);
        // non-writable columns.
        this.nonWriteColumnIndexes.add(vertexSearcher.getColumnIndex());
        // non-updatable columns.
        this.nonUpdateColumnIndexes.addAll(nonUpdateColumnIndexes);
        this.nonUpdateColumnIndexes.add(labelIndex);

        this.vertexKeyValueCount = (fieldNames.length - nonWriteColumnIndexes.size()) * 2;
    }

    @Override
    public void addToBatch(RowData record) {
        switch (record.getRowKind()) {
            case INSERT:
                Object[] keyValuePairs = mergeWithFieldNames(converter.toExternal(record));
                transaction.addVertex(keyValuePairs);
                break;
            case UPDATE_AFTER:
                Object[] values = converter.toExternal(record);
                Vertex vertex = vertexSearcher.search(values, transaction);
                for (int i = 0; i < values.length; i++) {
                    if (!nonWriteColumnIndexes.contains(i) || !nonUpdateColumnIndexes.contains(i)) {
                        vertex.property(fieldNames[i], values[i]);
                    }
                }
                break;
            case DELETE:
                vertexSearcher.search(converter.toExternal(record), transaction).remove();
                break;
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
        Object[] keyValuePairs = new Object[vertexKeyValueCount];
        int index = 0;
        for (int i = 0; i < values.length; i++) {
            if (nonWriteColumnIndexes.contains(i)) {
                continue;
            }

            int pos = index++ * 2;
            String fieldName = fieldNames[i];
            keyValuePairs[pos] = RESERVED_FIELDS.getOrDefault(fieldName, fieldName);
            keyValuePairs[pos + 1] = values[i];
        }
        return keyValuePairs;
    }
}

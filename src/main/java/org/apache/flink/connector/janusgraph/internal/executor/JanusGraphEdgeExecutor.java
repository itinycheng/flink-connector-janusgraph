package org.apache.flink.connector.janusgraph.internal.executor;

import org.apache.flink.connector.janusgraph.internal.converter.JanusGraphRowConverter;
import org.apache.flink.connector.janusgraph.internal.helper.VertexObjectSearcher;
import org.apache.flink.connector.janusgraph.options.JanusGraphOptions;
import org.apache.flink.table.data.RowData;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** JanusGraph edge executor. */
public class JanusGraphEdgeExecutor extends JanusGraphExecutor {

    private final String[] fieldNames;

    private final int labelIndex;

    private final VertexObjectSearcher fromVertexSearcher;

    private final VertexObjectSearcher toVertexSearcher;

    private final JanusGraphRowConverter converter;

    private final List<Integer> internalColumnIndexes;

    public JanusGraphEdgeExecutor(
            @Nonnull String[] fieldNames,
            @Nonnull Integer labelIndex,
            @Nonnull VertexObjectSearcher fromVertexSearcher,
            @Nonnull VertexObjectSearcher toVertexSearcher,
            @Nonnull JanusGraphRowConverter converter,
            @Nonnull JanusGraphOptions options) {
        checkArgument(labelIndex >= 0);

        this.labelIndex = labelIndex;
        this.fieldNames = checkNotNull(fieldNames);
        this.fromVertexSearcher = checkNotNull(fromVertexSearcher);
        this.toVertexSearcher = checkNotNull(toVertexSearcher);
        this.converter = checkNotNull(converter);
        this.maxRetries = checkNotNull(options.getMaxRetries());
        this.internalColumnIndexes =
                Arrays.asList(
                        labelIndex,
                        fromVertexSearcher.getVertexColumnIndex(),
                        toVertexSearcher.getVertexColumnIndex());
    }

    @Override
    public void addToBatch(RowData record) {
        switch (record.getRowKind()) {
            case INSERT:
                Object[] values = converter.toExternal(record);
                Vertex fromV = fromVertexSearcher.search(values, transaction);
                Vertex toV = toVertexSearcher.search(values, transaction);
                Edge edge = fromV.addEdge(values[labelIndex].toString(), toV);
                for (int i = 0; i < values.length; i++) {
                    if (!internalColumnIndexes.contains(i)) {
                        edge.property(fieldNames[i], values[i]);
                    }
                }
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
}

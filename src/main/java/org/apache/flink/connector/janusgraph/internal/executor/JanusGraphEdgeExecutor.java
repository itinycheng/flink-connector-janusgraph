package org.apache.flink.connector.janusgraph.internal.executor;

import org.apache.flink.connector.janusgraph.internal.converter.JanusGraphRowConverter;
import org.apache.flink.connector.janusgraph.internal.helper.ElementObjectSearcher;
import org.apache.flink.connector.janusgraph.options.JanusGraphOptions;
import org.apache.flink.table.data.RowData;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nonnull;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** JanusGraph edge executor. */
public class JanusGraphEdgeExecutor extends JanusGraphExecutor {

    private final String[] fieldNames;

    private final int labelIndex;

    private final ElementObjectSearcher<Edge> edgeSearcher;

    private final ElementObjectSearcher<Vertex> inVertexSearcher;

    private final ElementObjectSearcher<Vertex> outVertexSearcher;

    private final JanusGraphRowConverter converter;

    public JanusGraphEdgeExecutor(
            @Nonnull String[] fieldNames,
            @Nonnull Integer labelIndex,
            @Nonnull ElementObjectSearcher<Edge> edgeSearcher,
            @Nonnull ElementObjectSearcher<Vertex> inVertexSearcher,
            @Nonnull ElementObjectSearcher<Vertex> outVertexSearcher,
            @Nonnull JanusGraphRowConverter converter,
            @Nonnull List<Integer> nonUpdateColumnIndexes,
            @Nonnull JanusGraphOptions options) {
        super(options);

        checkArgument(labelIndex >= 0);
        this.labelIndex = labelIndex;
        this.fieldNames = checkNotNull(fieldNames);
        this.edgeSearcher = checkNotNull(edgeSearcher);
        this.inVertexSearcher = checkNotNull(inVertexSearcher);
        this.outVertexSearcher = checkNotNull(outVertexSearcher);
        this.converter = checkNotNull(converter);

        this.nonWriteColumnIndexes.add(edgeSearcher.getColumnIndex());
        this.nonWriteColumnIndexes.add(inVertexSearcher.getColumnIndex());
        this.nonWriteColumnIndexes.add(outVertexSearcher.getColumnIndex());

        this.nonUpdateColumnIndexes.addAll(nonUpdateColumnIndexes);
        this.nonUpdateColumnIndexes.add(labelIndex);
    }

    @Override
    public void addToBatch(RowData record) {
        Object[] values = converter.toExternal(record);
        switch (record.getRowKind()) {
            case INSERT:
                Vertex inV = inVertexSearcher.search(values, transaction);
                Vertex outV = outVertexSearcher.search(values, transaction);
                Edge created = inV.addEdge(values[labelIndex].toString(), outV);
                for (int i = 0; i < values.length; i++) {
                    if (!nonWriteColumnIndexes.contains(i)) {
                        created.property(fieldNames[i], values[i]);
                    }
                }
                break;
            case UPDATE_AFTER:
                Edge searched = edgeSearcher.search(values, transaction);
                for (int i = 0; i < values.length; i++) {
                    if (!nonWriteColumnIndexes.contains(i) || !nonUpdateColumnIndexes.contains(i)) {
                        searched.property(fieldNames[i], values[i]);
                    }
                }
                break;
            case DELETE:
                edgeSearcher.search(values, transaction).remove();
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
}

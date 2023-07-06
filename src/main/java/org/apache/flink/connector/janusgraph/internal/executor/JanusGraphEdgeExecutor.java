package org.apache.flink.connector.janusgraph.internal.executor;

import org.apache.flink.connector.janusgraph.internal.converter.JanusGraphRowConverter;
import org.apache.flink.connector.janusgraph.internal.helper.ElementObjectSearcher;
import org.apache.flink.connector.janusgraph.options.JanusGraphOptions;
import org.apache.flink.table.data.RowData;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.List;

import static org.apache.flink.connector.janusgraph.options.UpdateNotFoundStrategy.FAIL;
import static org.apache.flink.connector.janusgraph.options.UpdateNotFoundStrategy.INSERT;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** JanusGraph edge executor. */
public class JanusGraphEdgeExecutor extends JanusGraphExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(JanusGraphVertexExecutor.class);

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
        // non-writable columns.
        this.nonWriteColumnIndexes.add(labelIndex);
        this.nonWriteColumnIndexes.add(edgeSearcher.getColumnIndex());
        this.nonWriteColumnIndexes.add(inVertexSearcher.getColumnIndex());
        this.nonWriteColumnIndexes.add(outVertexSearcher.getColumnIndex());
        // non-updatable columns.
        this.nonUpdateColumnIndexes.addAll(nonUpdateColumnIndexes);
    }

    @Override
    protected void execute(RowData record, JanusGraphTransaction transaction) {
        Object[] values = converter.toExternal(record);
        Edge searched;
        switch (record.getRowKind()) {
            case INSERT:
                createEdge(values, transaction);
                break;
            case UPDATE_AFTER:
                searched = edgeSearcher.search(values, transaction);
                if (searched != null) {
                    updateEdge(searched, values);
                } else {
                    if (updateNotFoundStrategy == FAIL) {
                        throw new RuntimeException("Edge not found");
                    } else if (updateNotFoundStrategy == INSERT) {
                        createEdge(values, transaction);
                    } else {
                        LOG.debug("Not found edge: [{}], ignore update it", record);
                    }
                }
                break;
            case DELETE:
                searched = edgeSearcher.search(values, transaction);
                if (searched != null) {
                    searched.remove();
                }
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

    private void createEdge(Object[] values, JanusGraphTransaction transaction) {
        Vertex inV = inVertexSearcher.search(values, transaction);
        Vertex outV = outVertexSearcher.search(values, transaction);
        Edge created = outV.addEdge(values[labelIndex].toString(), inV);
        for (int i = 0; i < values.length; i++) {
            if (!nonWriteColumnIndexes.contains(i)) {
                created.property(fieldNames[i], values[i]);
            }
        }
    }

    private void updateEdge(Edge edge, Object[] values) {
        for (int i = 0; i < values.length; i++) {
            if (!nonWriteColumnIndexes.contains(i) && !nonUpdateColumnIndexes.contains(i)) {
                edge.property(fieldNames[i], values[i]);
            }
        }
    }
}

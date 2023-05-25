package org.apache.flink.connector.janusgraph.internal.helper;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphTransaction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Search vertex by id. */
public class VertexByIdSearcher implements ElementObjectSearcher<Vertex> {

    private final int vertexIdIndex;

    public VertexByIdSearcher(int vertexIdIndex, @Nonnull LogicalType vertexIdType) {
        checkArgument(LogicalTypeRoot.BIGINT.equals(vertexIdType.getTypeRoot()));
        checkArgument(vertexIdIndex >= 0);
        this.vertexIdIndex = vertexIdIndex;
    }

    @Nullable
    @Override
    public Vertex search(Object[] rowData, JanusGraphTransaction transaction) {
        GraphTraversal<Vertex, Vertex> traversal =
                transaction.traversal().V(rowData[vertexIdIndex]);
        return traversal.hasNext() ? traversal.next() : null;
    }

    @Override
    public int getColumnIndex() {
        return vertexIdIndex;
    }
}

package org.apache.flink.connector.janusgraph.internal.helper;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphTransaction;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Search vertex by id. */
public class VertexByIdSearcher implements ElementObjectSearcher<Vertex> {

    private final int vertexColumnIndex;

    public VertexByIdSearcher(int vertexColumnIndex, @Nonnull LogicalType vertexColumnType) {
        checkArgument(LogicalTypeRoot.BIGINT.equals(vertexColumnType.getTypeRoot()));
        checkArgument(vertexColumnIndex >= 0);
        this.vertexColumnIndex = vertexColumnIndex;
    }

    @Override
    public Vertex search(Object[] rowData, JanusGraphTransaction transaction) {
        return transaction.traversal().V(rowData[vertexColumnIndex]).next();
    }

    @Override
    public int getColumnIndex() {
        return vertexColumnIndex;
    }
}

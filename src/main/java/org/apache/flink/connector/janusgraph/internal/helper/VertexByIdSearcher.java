package org.apache.flink.connector.janusgraph.internal.helper;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphTransaction;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Search vertex by id. */
public class VertexByIdSearcher extends VertexObjectSearcher {

    private final String vertexColumnName;

    private final LogicalType vertexColumnType;

    public VertexByIdSearcher(
            @Nonnull String vertexColumnName,
            @Nonnull LogicalType vertexColumnType,
            int vertexColumnIndex) {
        super(vertexColumnIndex);
        checkArgument(LogicalTypeRoot.BIGINT.equals(vertexColumnType.getTypeRoot()));

        this.vertexColumnName = checkNotNull(vertexColumnName);
        this.vertexColumnType = checkNotNull(vertexColumnType);
    }

    @Override
    public Vertex search(Object vertexInfo, JanusGraphTransaction transaction) {
        return transaction.traversal().V(vertexInfo).next();
    }
}

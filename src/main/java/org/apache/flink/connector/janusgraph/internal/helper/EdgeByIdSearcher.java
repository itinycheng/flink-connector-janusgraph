package org.apache.flink.connector.janusgraph.internal.helper;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.janusgraph.core.JanusGraphTransaction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Search edge by id. */
public class EdgeByIdSearcher implements ElementObjectSearcher<Edge> {

    private final int edgeIdIndex;

    public EdgeByIdSearcher(@Nonnull LogicalType edgeIdType, int edgeIdIndex) {
        checkArgument(LogicalTypeRoot.BIGINT.equals(edgeIdType.getTypeRoot()));
        checkArgument(edgeIdIndex >= 0);

        this.edgeIdIndex = edgeIdIndex;
    }

    @Nullable
    @Override
    public Edge search(Object[] rowData, JanusGraphTransaction transaction) {
        GraphTraversal<Edge, Edge> traversal = transaction.traversal().E(rowData[edgeIdIndex]);
        return traversal.hasNext() ? traversal.next() : null;
    }

    @Override
    public int getColumnIndex() {
        return edgeIdIndex;
    }
}

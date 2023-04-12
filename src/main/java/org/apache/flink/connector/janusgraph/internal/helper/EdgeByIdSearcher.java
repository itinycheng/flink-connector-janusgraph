package org.apache.flink.connector.janusgraph.internal.helper;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.janusgraph.core.JanusGraphTransaction;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Search edge by id. */
public class EdgeByIdSearcher implements ElementObjectSearcher<Edge> {

    private final int edgeColumnIndex;

    public EdgeByIdSearcher(@Nonnull LogicalType edgeColumnType, int edgeColumnIndex) {
        checkArgument(LogicalTypeRoot.BIGINT.equals(edgeColumnType.getTypeRoot()));
        checkArgument(edgeColumnIndex >= 0);

        this.edgeColumnIndex = edgeColumnIndex;
    }

    @Override
    public Edge search(Object[] rowData, JanusGraphTransaction transaction) {
        return transaction.traversal().E(rowData[edgeColumnIndex]).next();
    }

    @Override
    public int getColumnIndex() {
        return edgeColumnIndex;
    }
}

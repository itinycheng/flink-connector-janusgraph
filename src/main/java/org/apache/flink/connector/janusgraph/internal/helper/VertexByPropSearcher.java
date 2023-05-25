package org.apache.flink.connector.janusgraph.internal.helper;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphTransaction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_ID;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_LABEL;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Searche vertex by properties. */
public class VertexByPropSearcher implements ElementObjectSearcher<Vertex> {

    private final int vertexColumnIndex;

    private final String[] vertexColumnFieldNames;

    public VertexByPropSearcher(int vertexColumnIndex, @Nonnull LogicalType vertexColumnType) {
        checkArgument(LogicalTypeRoot.ROW.equals(vertexColumnType.getTypeRoot()));
        checkArgument(vertexColumnIndex >= 0);

        this.vertexColumnIndex = vertexColumnIndex;
        this.vertexColumnFieldNames =
                ((RowType) vertexColumnType).getFieldNames().toArray(new String[0]);
    }

    @Nullable
    @Override
    public Vertex search(Object[] rowData, JanusGraphTransaction transaction) {
        Row vertexRow = (Row) rowData[vertexColumnIndex];
        GraphTraversal<Vertex, Vertex> traversal = transaction.traversal().V();
        for (int i = 0; i < vertexRow.getArity(); i++) {
            Object fieldValue = vertexRow.getField(i);
            if (fieldValue == null) {
                continue;
            }

            String fieldName = vertexColumnFieldNames[i];
            if (KEYWORD_ID.equals(fieldName)) {
                traversal = traversal.hasId(fieldValue);
            } else if (KEYWORD_LABEL.equals(fieldName)) {
                traversal = traversal.hasLabel((String) fieldValue);
            } else {
                traversal = traversal.has(fieldName, fieldValue);
            }
        }

        return traversal.hasNext() ? traversal.next() : null;
    }

    @Override
    public int getColumnIndex() {
        return vertexColumnIndex;
    }
}

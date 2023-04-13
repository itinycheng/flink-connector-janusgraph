package org.apache.flink.connector.janusgraph.internal.helper;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphTransaction;

import javax.annotation.Nonnull;

import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_LABEL;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Search edge by id. */
public class EdgeByPropSearcher implements ElementObjectSearcher<Edge> {

    private final int inVertexIndex;
    private final int outVertexIndex;
    private final int edgeIdIndex;
    private final int edgeLabelIndex;

    private final String[] edgeIdFieldNames;

    public EdgeByPropSearcher(
            @Nonnull LogicalType edgeIdType,
            int edgeIdIndex,
            int edgeLabelIndex,
            int inVertexIndex,
            int outVertexIndex) {
        checkArgument(LogicalTypeRoot.ROW.equals(edgeIdType.getTypeRoot()));
        checkArgument(edgeIdIndex >= 0);
        checkArgument(edgeLabelIndex >= 0);
        checkArgument(inVertexIndex >= 0);
        checkArgument(outVertexIndex >= 0);

        this.edgeLabelIndex = edgeLabelIndex;
        this.edgeIdIndex = edgeIdIndex;
        this.inVertexIndex = inVertexIndex;
        this.outVertexIndex = outVertexIndex;
        this.edgeIdFieldNames = ((RowType) edgeIdType).getFieldNames().toArray(new String[0]);
    }

    @Nonnull
    @Override
    public Edge search(Object[] rowData, JanusGraphTransaction transaction) {
        GraphTraversal<Vertex, Object> traversal =
                transaction
                        .traversal()
                        .V()
                        .in()
                        .where(getVertexTraversal(rowData[inVertexIndex]))
                        .outE(rowData[edgeLabelIndex].toString())
                        .as("e")
                        .inV()
                        .where(getVertexTraversal(rowData[outVertexIndex]))
                        .select("e");

        Row edgeIdRow = (Row) rowData[edgeIdIndex];
        for (int i = 0; i < edgeIdRow.getArity(); i++) {
            final String key = edgeIdFieldNames[i];
            final Object value = edgeIdRow.getField(i);
            if (!KEYWORD_LABEL.equals(key) && value != null) {
                traversal = traversal.has(key, value);
            }
        }

        return (Edge) traversal.next();
    }

    @Override
    public int getColumnIndex() {
        return edgeIdIndex;
    }

    private GraphTraversal<Vertex, Vertex> getVertexTraversal(Object vertexData) {
        if (vertexData instanceof Number) {
            return __.V(((Number) vertexData).longValue());
        } else {
            GraphTraversal<Vertex, Vertex> traversal = __.V();
            Row vertexRow = (Row) vertexData;
            for (int i = 0; i < vertexRow.getArity(); i++) {
                String fieldName = edgeIdFieldNames[i];
                Object fieldValue = vertexRow.getField(i);
                if (fieldValue != null) {
                    if (KEYWORD_LABEL.equals(fieldName)) {
                        traversal = traversal.hasLabel(fieldValue.toString());
                    } else {
                        traversal = traversal.has(fieldName, fieldValue);
                    }
                }
            }
            return traversal;
        }
    }
}

package org.apache.flink.connector.janusgraph.internal.helper;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphTransaction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_LABEL;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Search edge by id. */
public class EdgeByPropSearcher implements ElementObjectSearcher<Edge> {

    private final int inVertexIndex;
    private final int outVertexIndex;
    private final int edgeIdIndex;
    private final int edgeLabelIndex;

    private final String[] edgeIdFieldNames;

    private final String[] inVertexFieldNames;

    private final String[] outVertexFieldNames;

    public EdgeByPropSearcher(
            int edgeIdIndex,
            int edgeLabelIndex,
            int inVertexIndex,
            int outVertexIndex,
            @Nonnull LogicalType edgeIdType,
            @Nonnull LogicalType inVertexType,
            @Nonnull LogicalType outVertexType) {
        checkArgument(LogicalTypeRoot.ROW.equals(edgeIdType.getTypeRoot()));
        checkArgument(edgeIdIndex >= 0);
        checkArgument(edgeLabelIndex >= 0);
        checkArgument(inVertexIndex >= 0);
        checkArgument(outVertexIndex >= 0);

        this.edgeIdIndex = edgeIdIndex;
        this.edgeLabelIndex = edgeLabelIndex;
        this.inVertexIndex = inVertexIndex;
        this.outVertexIndex = outVertexIndex;
        this.edgeIdFieldNames = ((RowType) edgeIdType).getFieldNames().toArray(new String[0]);
        this.inVertexFieldNames =
                inVertexType instanceof RowType
                        ? ((RowType) inVertexType).getFieldNames().toArray(new String[0])
                        : new String[0];
        this.outVertexFieldNames =
                outVertexType instanceof RowType
                        ? ((RowType) outVertexType).getFieldNames().toArray(new String[0])
                        : new String[0];
    }

    @Nullable
    @Override
    public Edge search(Object[] rowData, JanusGraphTransaction transaction) {
        GraphTraversal<Vertex, ?> traversal = transaction.traversal().V();
        traversal =
                appendVertexTraversal(traversal, rowData[outVertexIndex], outVertexFieldNames)
                        .outE(rowData[edgeLabelIndex].toString())
                        .as("e")
                        .inV();

        appendVertexTraversal(traversal, rowData[inVertexIndex], inVertexFieldNames).select("e");

        Row edgeIdRow = (Row) rowData[edgeIdIndex];
        if (edgeIdRow != null) {
            for (int i = 0; i < edgeIdRow.getArity(); i++) {
                final String key = edgeIdFieldNames[i];
                final Object value = edgeIdRow.getField(i);
                if (!KEYWORD_LABEL.equals(key) && value != null) {
                    traversal = traversal.has(key, value);
                }
            }
        }

        return (Edge) (traversal.hasNext() ? traversal.next() : null);
    }

    @Override
    public int getColumnIndex() {
        return edgeIdIndex;
    }

    private GraphTraversal<Vertex, ?> appendVertexTraversal(
            GraphTraversal<Vertex, ?> vertexTraversal,
            Object vertexData,
            String[] vertexFieldNames) {
        if (vertexData instanceof Number) {
            return vertexTraversal.hasId(((Number) vertexData).longValue());
        } else {
            Row vertexRow = (Row) vertexData;
            for (int i = 0; i < vertexRow.getArity(); i++) {
                String fieldName = vertexFieldNames[i];
                Object fieldValue = vertexRow.getField(i);
                if (fieldValue != null) {
                    if (KEYWORD_LABEL.equals(fieldName)) {
                        vertexTraversal = vertexTraversal.hasLabel(fieldValue.toString());
                    } else {
                        vertexTraversal = vertexTraversal.has(fieldName, fieldValue);
                    }
                }
            }
            return vertexTraversal;
        }
    }
}

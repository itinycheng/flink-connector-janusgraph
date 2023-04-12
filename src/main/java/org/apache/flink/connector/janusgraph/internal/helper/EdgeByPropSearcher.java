package org.apache.flink.connector.janusgraph.internal.helper;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphTransaction;

import javax.annotation.Nonnull;

import java.util.Map;

import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_LABEL;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Search edge by id. */
public class EdgeByPropSearcher implements ElementObjectSearcher<Edge> {

    private final int inVertexIndex;
    private final int outVertexIndex;
    private final int edgeColumnIndex;
    private final int labelIndex;

    public EdgeByPropSearcher(
            @Nonnull LogicalType edgeColumnType,
            int edgeColumnIndex,
            int labelIndex,
            int inVertexIndex,
            int outVertexIndex) {
        checkArgument(LogicalTypeRoot.MAP.equals(edgeColumnType.getTypeRoot()));
        checkArgument(edgeColumnIndex >= 0);
        checkArgument(labelIndex >= 0);
        checkArgument(inVertexIndex >= 0);
        checkArgument(outVertexIndex >= 0);

        this.labelIndex = labelIndex;
        this.edgeColumnIndex = edgeColumnIndex;
        this.inVertexIndex = inVertexIndex;
        this.outVertexIndex = outVertexIndex;
    }

    @Override
    public Edge search(Object[] rowData, JanusGraphTransaction transaction) {
        GraphTraversal<Vertex, Object> traversal =
                transaction
                        .traversal()
                        .V()
                        .inV()
                        .where(getVertexTraversal(rowData[inVertexIndex], transaction))
                        .outE(rowData[labelIndex].toString())
                        .as("e")
                        .inV()
                        .where(getVertexTraversal(rowData[outVertexIndex], transaction))
                        .select("e");

        Map<String, Object> edgePropMap = (Map<String, Object>) rowData[edgeColumnIndex];
        for (Map.Entry<String, Object> entry : edgePropMap.entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();
            if (!KEYWORD_LABEL.equals(key) && value != null) {
                traversal = traversal.has(key, value);
            }
        }

        return (Edge) traversal.next();
    }

    @Override
    public int getColumnIndex() {
        return edgeColumnIndex;
    }

    private GraphTraversal<Vertex, Vertex> getVertexTraversal(
            Object vertexData, JanusGraphTransaction transaction) {
        if (vertexData instanceof Number) {
            return transaction.traversal().V(((Number) vertexData).longValue());
        } else {
            Map<String, Object> vertexDataMap = (Map<String, Object>) vertexData;
            GraphTraversal<Vertex, Vertex> traversal = transaction.traversal().V();
            Object label = vertexDataMap.get(KEYWORD_LABEL);
            if (label != null) {
                traversal = traversal.hasLabel(label.toString());
            }

            for (Map.Entry<String, Object> entry : vertexDataMap.entrySet()) {
                final String key = entry.getKey();
                final Object value = entry.getValue();
                if (!KEYWORD_LABEL.equals(key) && value != null) {
                    traversal = traversal.has(key, value);
                }
            }

            return traversal;
        }
    }
}

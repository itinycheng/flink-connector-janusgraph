package org.apache.flink.connector.janusgraph.internal.helper;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphTransaction;

import javax.annotation.Nonnull;

import java.util.Map;

import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_LABEL;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Searche vertex by properties. */
public class VertexByPropSearcher implements ElementObjectSearcher<Vertex> {

    private final int vertexColumnIndex;

    private final int labelIndex;

    public VertexByPropSearcher(
            @Nonnull LogicalType vertexColumnType, int vertexColumnIndex, int labelIndex) {
        checkArgument(LogicalTypeRoot.MAP.equals(vertexColumnType.getTypeRoot()));
        checkArgument(
                LogicalTypeRoot.VARCHAR.equals(
                        vertexColumnType.getChildren().get(0).getTypeRoot()));
        checkArgument(vertexColumnIndex >= 0);
        checkArgument(labelIndex >= 0);

        this.vertexColumnIndex = vertexColumnIndex;
        this.labelIndex = labelIndex;
    }

    @Override
    public Vertex search(Object[] rowData, JanusGraphTransaction transaction) {
        Map<String, Object> vertexPropMap = (Map<String, Object>) rowData[vertexColumnIndex];
        GraphTraversal<Vertex, Vertex> traversal =
                transaction.traversal().V().hasLabel(rowData[labelIndex].toString());

        for (Map.Entry<String, Object> entry : vertexPropMap.entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();
            if (!KEYWORD_LABEL.equals(key) && value != null) {
                traversal = traversal.has(key, value);
            }
        }

        return traversal.next();
    }

    @Override
    public int getColumnIndex() {
        return vertexColumnIndex;
    }
}

package org.apache.flink.connector.janusgraph.internal.helper;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphTransaction;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.Objects;

import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_LABEL;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Searche vertex by properties. */
public class VertexByPropSearcher extends VertexObjectSearcher {

    private final String vertexColumnName;
    private final LogicalType vertexColumnType;

    public VertexByPropSearcher(
            @Nonnull String vertexColumnName,
            @Nonnull LogicalType vertexColumnType,
            int vertexColumnIndex) {
        super(vertexColumnIndex);
        checkArgument(LogicalTypeRoot.MAP.equals(vertexColumnType.getTypeRoot()));
        checkArgument(
                LogicalTypeRoot.VARCHAR.equals(
                        vertexColumnType.getChildren().get(0).getTypeRoot()));

        this.vertexColumnName = checkNotNull(vertexColumnName);
        this.vertexColumnType = checkNotNull(vertexColumnType);
    }

    @Override
    public Vertex search(Object vertexInfo, JanusGraphTransaction transaction) {
        Map<String, Object> vertexPropMap = (Map<String, Object>) vertexInfo;

        GraphTraversal<Vertex, Vertex> traversal = transaction.traversal().V();
        Object label = vertexPropMap.get(KEYWORD_LABEL);
        if (label != null) {
            traversal.hasLabel(label.toString());
        }

        vertexPropMap.entrySet().stream()
                .filter(entry -> !Objects.equals(entry.getKey(), KEYWORD_LABEL))
                .filter(entry -> entry.getValue() != null)
                .forEach(entry -> traversal.has(entry.getKey(), entry.getValue()));
        return traversal.next();
    }
}

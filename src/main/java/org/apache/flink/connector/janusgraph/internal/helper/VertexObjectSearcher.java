package org.apache.flink.connector.janusgraph.internal.helper;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphTransaction;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Vertex searcher. */
public abstract class VertexObjectSearcher implements Serializable {

    protected final int vertexColumnIndex;

    public VertexObjectSearcher(int vertexColumnIndex) {
        checkArgument(vertexColumnIndex >= 0);
        this.vertexColumnIndex = vertexColumnIndex;
    }

    public Vertex search(Object[] rowData, JanusGraphTransaction transaction) {
        return search(rowData[vertexColumnIndex], transaction);
    }

    public abstract Vertex search(Object vertexInfo, JanusGraphTransaction transaction);

    public int getVertexColumnIndex() {
        return vertexColumnIndex;
    }
}

package org.apache.flink.connector.janusgraph.internal.connection;

import org.apache.commons.configuration2.Configuration;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** JanusGraph connection. */
public class JanusGraphConnection {

    private static final Map<SameConfiguration, JanusGraph> CACHED_CLIENTS =
            new ConcurrentHashMap<>();

    private final SameConfiguration configuration;

    // Thread safety.
    private final JanusGraph graph;

    public JanusGraphConnection(@Nonnull Configuration configuration) {
        this.configuration = new SameConfiguration(configuration);
        this.graph =
                CACHED_CLIENTS.computeIfAbsent(
                        this.configuration,
                        k ->
                                JanusGraphFactory.open(
                                        new CommonsConfiguration(k.getConfiguration())));
    }

    public JanusGraphTransaction newTransaction() {
        return graph.newTransaction();
    }

    public void close() {
        JanusGraph janusGraph = CACHED_CLIENTS.remove(configuration);
        if (janusGraph != null) {
            janusGraph.close();
        }
        graph.close();
    }
}

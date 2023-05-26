package org.apache.flink.connector.janusgraph.internal.connection;

import org.apache.commons.configuration2.Configuration;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** JanusGraph connection. */
public class JanusGraphConnection {

    private static final Map<SameConfiguration, JanusGraph> CACHED_JANUS_GRAPH_CLIENTS =
            new ConcurrentHashMap<>();

    private final Configuration configuration;

    // Thread safety.
    private final JanusGraph graph;

    public JanusGraphConnection(@Nonnull Configuration configuration) {
        this.configuration = checkNotNull(configuration);
        this.graph =
                CACHED_JANUS_GRAPH_CLIENTS.computeIfAbsent(
                        new SameConfiguration(configuration),
                        k ->
                                JanusGraphFactory.open(
                                        new CommonsConfiguration(k.getConfiguration())));
    }

    public JanusGraphTransaction newTransaction() {
        return graph.newTransaction();
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void close() {
        graph.close();
    }
}

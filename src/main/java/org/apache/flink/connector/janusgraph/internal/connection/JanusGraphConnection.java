package org.apache.flink.connector.janusgraph.internal.connection;

import org.apache.commons.configuration2.Configuration;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;

/** JanusGraph connection. */
public class JanusGraphConnection {

    private final Configuration configuration;

    // Thread safety.
    private final JanusGraph graph;

    public JanusGraphConnection(Configuration configuration) {
        this.configuration = configuration;
        this.graph = JanusGraphFactory.open(new CommonsConfiguration(configuration));
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

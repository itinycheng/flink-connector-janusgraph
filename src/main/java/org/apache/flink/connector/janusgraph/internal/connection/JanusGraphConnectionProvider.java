package org.apache.flink.connector.janusgraph.internal.connection;

import org.apache.flink.connector.janusgraph.options.JanusGraphOptions;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

/** Connection Provider. */
public class JanusGraphConnectionProvider implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(JanusGraphConnectionProvider.class);

    private final JanusGraphOptions options;

    private final Properties configProperties;

    private transient JanusGraphConnection connection;

    public JanusGraphConnectionProvider(JanusGraphOptions options, Properties configProperties) {
        this.options = options;
        this.configProperties = configProperties;
    }

    public synchronized JanusGraphConnection getOrCreateConnection() {
        if (connection != null) {
            return connection;
        }

        LOG.info("connecting to {}, database {}", options, configProperties);
        PropertiesConfiguration configuration = new PropertiesConfiguration();
        configProperties.forEach((k, v) -> configuration.addProperty(k.toString(), v));
        configuration.addProperty("gremlin.graph", options.getFactory());
        configuration.addProperty(
                "storage.hostname",
                Arrays.stream(options.getHosts().split(",")).collect(Collectors.toList()));
        configuration.addProperty("storage.port", options.getPort());
        configuration.addProperty("storage.backend", options.getBackendType().name);
        configuration.addProperty("storage.username", options.getUsername());
        configuration.addProperty("storage.password", options.getPassword());
        configuration.addProperty("storage.hbase.table", options.getTableName());
        this.connection = new JanusGraphConnection(configuration);
        return this.connection;
    }

    public synchronized void close() {
        if (this.connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                LOG.warn("JanusGraph connection could not be closed.", e);
            } finally {
                connection = null;
            }
        }
    }
}

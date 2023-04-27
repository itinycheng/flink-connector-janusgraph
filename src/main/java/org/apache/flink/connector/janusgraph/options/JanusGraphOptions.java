package org.apache.flink.connector.janusgraph.options;

import org.apache.flink.connector.janusgraph.config.BackendType;
import org.apache.flink.connector.janusgraph.config.TableType;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;

/** Options for JanusGraph connector. */
public class JanusGraphOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String factory;
    private final String hosts;
    private final Integer port;
    private final BackendType backendType;
    private final String username;
    private final String password;
    private final String tableName;
    private final TableType tableType;

    private final int batchSize;

    private final Duration flushInterval;

    private final int maxRetries;

    private final Integer parallelism;

    private final String[] nonUpdateColumns;

    public JanusGraphOptions(
            String factory,
            String hosts,
            Integer port,
            BackendType backendType,
            String username,
            String password,
            String tableName,
            TableType tableType,
            int batchSize,
            Duration flushInterval,
            int maxRetries,
            Integer parallelism,
            String[] nonUpdateColumns) {
        this.factory = factory;
        this.hosts = hosts;
        this.port = port;
        this.backendType = backendType;
        this.username = username;
        this.password = password;
        this.tableName = tableName;
        this.tableType = tableType;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.maxRetries = maxRetries;
        this.parallelism = parallelism;
        this.nonUpdateColumns = nonUpdateColumns;
    }

    public String getFactory() {
        return factory;
    }

    public String getHosts() {
        return hosts;
    }

    public Integer getPort() {
        return port;
    }

    public BackendType getBackendType() {
        return backendType;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getTableName() {
        return tableName;
    }

    public TableType getTableType() {
        return tableType;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Duration getFlushInterval() {
        return flushInterval;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public String[] getNonUpdateColumns() {
        return nonUpdateColumns;
    }

    @Override
    public String toString() {
        return "JanusGraphOptions{"
                + "factory='"
                + factory
                + '\''
                + ", hosts='"
                + hosts
                + '\''
                + ", port="
                + port
                + ", backendType="
                + backendType
                + ", username='"
                + username
                + '\''
                + ", password='"
                + password
                + '\''
                + ", tableName='"
                + tableName
                + '\''
                + ", tableType="
                + tableType
                + ", batchSize="
                + batchSize
                + ", flushInterval="
                + flushInterval
                + ", maxRetries="
                + maxRetries
                + ", parallelism="
                + parallelism
                + ", nonUpdateColumns="
                + Arrays.toString(nonUpdateColumns)
                + '}';
    }

    /** Builder for {@link JanusGraphOptions}. */
    public static class Builder {
        private String factory;
        private String hosts;
        private Integer port;
        private BackendType backendType;
        private String username;
        private String password;
        private String tableName;
        private TableType tableType;
        private int batchSize;
        private Duration flushInterval;
        private int maxRetries;
        private Integer parallelism;

        private String[] nonUpdateColumns;

        public Builder() {}

        public Builder setFactory(String factory) {
            this.factory = factory;
            return this;
        }

        public Builder setHosts(String hosts) {
            this.hosts = hosts;
            return this;
        }

        public Builder setPort(Integer port) {
            this.port = port;
            return this;
        }

        public Builder setBackendType(BackendType backendType) {
            this.backendType = backendType;
            return this;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setTableType(TableType tableType) {
            this.tableType = tableType;
            return this;
        }

        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder setFlushInterval(Duration flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public Builder setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder setParallelism(Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public Builder setNonUpdateColumns(String nonUpdateColumns) {
            if (nonUpdateColumns == null || nonUpdateColumns.isEmpty()) {
                this.nonUpdateColumns = new String[0];
            } else {
                this.nonUpdateColumns = nonUpdateColumns.split(",");
            }
            return this;
        }

        public JanusGraphOptions build() {
            return new JanusGraphOptions(
                    factory,
                    hosts,
                    port,
                    backendType,
                    username,
                    password,
                    tableName,
                    tableType,
                    batchSize,
                    flushInterval,
                    maxRetries,
                    parallelism,
                    nonUpdateColumns);
        }
    }
}

package org.apache.flink.connector.janusgraph.config;

/** Backend Type. */
public enum BackendType {
    HBASE("hbase"),
    CQL("cql");

    public final String name;

    BackendType(String name) {
        this.name = name;
    }
}

package org.apache.flink.connector.janusgraph.options;

/** update not found strategy. */
public enum UpdateNotFoundStrategy {
    FAIL,
    IGNORE,
    INSERT
}

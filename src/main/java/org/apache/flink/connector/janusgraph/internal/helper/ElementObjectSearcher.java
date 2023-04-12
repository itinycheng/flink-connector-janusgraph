package org.apache.flink.connector.janusgraph.internal.helper;

import org.janusgraph.core.JanusGraphTransaction;

import java.io.Serializable;

/** Edge searcher. */
public interface ElementObjectSearcher<T> extends Serializable {
    T search(Object[] rowData, JanusGraphTransaction transaction);

    int getColumnIndex();
}

package org.apache.flink.connector.janusgraph.internal.helper;

import org.janusgraph.core.JanusGraphTransaction;

import javax.annotation.Nullable;

import java.io.Serializable;

/** Edge searcher. */
public interface ElementObjectSearcher<T> extends Serializable {
    @Nullable
    T search(Object[] rowData, JanusGraphTransaction transaction);

    int getColumnIndex();
}

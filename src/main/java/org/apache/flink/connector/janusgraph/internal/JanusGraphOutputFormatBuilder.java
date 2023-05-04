package org.apache.flink.connector.janusgraph.internal;

import org.apache.flink.connector.janusgraph.internal.connection.JanusGraphConnectionProvider;
import org.apache.flink.connector.janusgraph.options.JanusGraphOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;
import java.util.function.Function;

import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_ID;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.KEYWORD_LABEL;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** JanusGraph output format builder. */
public class JanusGraphOutputFormatBuilder implements Serializable {

    private String[] fieldNames;

    private DataType[] fieldTypes;

    private LogicalType[] logicalTypes;

    private String[] primaryKeys;

    private JanusGraphOptions options;

    private Properties configProperties;

    public JanusGraphOutputFormatBuilder setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
        return this;
    }

    public JanusGraphOutputFormatBuilder setFieldTypes(DataType[] fieldTypes) {
        this.fieldTypes = fieldTypes;
        this.logicalTypes =
                Arrays.stream(fieldTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new);
        return this;
    }

    public JanusGraphOutputFormatBuilder setPrimaryKeys(String[] primaryKeys) {
        this.primaryKeys = primaryKeys;
        return this;
    }

    public JanusGraphOutputFormatBuilder setOptions(JanusGraphOptions options) {
        this.options = options;
        return this;
    }

    public JanusGraphOutputFormatBuilder setConfigProperties(Properties configProperties) {
        this.configProperties = configProperties;
        return this;
    }

    public JanusGraphOutputFormat<RowData> build() {
        checkNotNull(options);
        checkNotNull(fieldNames);
        checkNotNull(fieldTypes);
        checkNotNull(primaryKeys);
        validateInternalColumns();

        return new JanusGraphOutputFormat<>(
                new JanusGraphConnectionProvider(options, configProperties),
                primaryKeys,
                fieldNames,
                logicalTypes,
                (Function<RowData, RowData> & Serializable) rowData -> rowData,
                options);
    }

    /** Table must contain columns of id, label.<br> */
    private void validateInternalColumns() {
        if (!ArrayUtils.contains(fieldNames, KEYWORD_LABEL)
                || !ArrayUtils.contains(fieldNames, KEYWORD_ID)) {
            throw new RuntimeException(
                    String.format(
                            "%s table must contains columns of %s and %s",
                            options.getTableType(), KEYWORD_LABEL, KEYWORD_ID));
        } else {
            throw new RuntimeException("Unknown table type: " + options.getTableType());
        }
    }
}

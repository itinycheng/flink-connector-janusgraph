package org.apache.flink.connector.janusgraph.internal;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.janusgraph.internal.connection.JanusGraphConnectionProvider;
import org.apache.flink.connector.janusgraph.options.JanusGraphOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** JanusGraph output format builder. */
public class JanusGraphOutputFormatBuilder implements Serializable {

    private String[] fieldNames;

    private DataType[] fieldTypes;

    private LogicalType[] logicalTypes;

    private String[] primaryKeys;

    private JanusGraphOptions options;

    private Properties configProperties;

    private TypeInformation<RowData> rowDataTypeInformation;

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

    public JanusGraphOutputFormatBuilder setRowDataTypeInformation(
            TypeInformation<RowData> rowDataTypeInformation) {
        this.rowDataTypeInformation = rowDataTypeInformation;
        return this;
    }

    public JanusGraphOutputFormat<RowData> build() {
        checkNotNull(options);
        checkNotNull(fieldNames);
        checkNotNull(fieldTypes);
        checkNotNull(primaryKeys);

        // vertex must have columns of v_id, label.
        // edge must have columns of from_v_id, to_v_id, label.

        return new JanusGraphOutputFormat<>(
                new JanusGraphConnectionProvider(options, configProperties),
                primaryKeys,
                fieldNames,
                logicalTypes,
                null,
                options);
    }
}

package org.apache.flink.connector.janusgraph.sink;

import org.apache.flink.connector.janusgraph.internal.JanusGraphOutputFormat;
import org.apache.flink.connector.janusgraph.internal.JanusGraphOutputFormatBuilder;
import org.apache.flink.connector.janusgraph.options.JanusGraphOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Properties;

/** JanusGraph table sink. */
public class JanusGraphDynamicTableSink implements DynamicTableSink {

    private final JanusGraphOptions options;

    private final Properties configProperties;

    private final String[] primaryKeys;

    private final DataType physicalRowDataType;

    public JanusGraphDynamicTableSink(
            @Nonnull JanusGraphOptions options,
            @Nonnull Properties configProperties,
            @Nonnull String[] primaryKeys,
            @Nonnull DataType physicalRowDataType) {
        this.options = options;
        this.configProperties = configProperties;
        this.primaryKeys = primaryKeys;
        this.physicalRowDataType = physicalRowDataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        Preconditions.checkState(
                ChangelogMode.insertOnly().equals(requestedMode) || primaryKeys.length > 0,
                "Please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        JanusGraphOutputFormat<RowData> outputFormat =
                new JanusGraphOutputFormatBuilder()
                        .setOptions(options)
                        .setConfigProperties(configProperties)
                        .setFieldNames(
                                DataType.getFieldNames(physicalRowDataType).toArray(new String[0]))
                        .setFieldTypes(
                                DataType.getFieldDataTypes(physicalRowDataType)
                                        .toArray(new DataType[0]))
                        .setPrimaryKeys(primaryKeys)
                        .build();

        return SinkFunctionProvider.of(
                new JanusGraphSinkFunction<>(outputFormat), options.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new JanusGraphDynamicTableSink(
                options, configProperties, primaryKeys, physicalRowDataType);
    }

    @Override
    public String asSummaryString() {
        return "JanusGraph sink table";
    }
}

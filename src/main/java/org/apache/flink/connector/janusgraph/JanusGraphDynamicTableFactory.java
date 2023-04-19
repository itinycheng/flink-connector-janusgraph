package org.apache.flink.connector.janusgraph;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.janusgraph.options.JanusGraphOptions;
import org.apache.flink.connector.janusgraph.sink.JanusGraphDynamicTableSink;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.IDENTIFIER;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.PROPERTIES_PREFIX;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfigOptions.BACKEND_TYPE;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfigOptions.FACTORY;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfigOptions.HOSTS;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfigOptions.PASSWORD;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfigOptions.PORT;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfigOptions.SINK_BATCH_SIZE;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfigOptions.SINK_FLUSH_INTERVAL;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfigOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfigOptions.SINK_NON_UPDATE_COLUMNS;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfigOptions.SINK_PARALLELISM;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfigOptions.TABLE_NAME;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfigOptions.TABLE_TYPE;
import static org.apache.flink.connector.janusgraph.config.JanusGraphConfigOptions.USERNAME;
import static org.apache.flink.connector.janusgraph.util.JanusGraphUtil.getJanusGraphProperties;

/** Factory for creating JanusGraph Tables. */
public class JanusGraphDynamicTableFactory implements DynamicTableSinkFactory {

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(PROPERTIES_PREFIX);

        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        String[] primaryKeys =
                catalogTable
                        .getResolvedSchema()
                        .getPrimaryKey()
                        .map(UniqueConstraint::getColumns)
                        .map(keys -> keys.toArray(new String[0]))
                        .orElse(new String[0]);
        return new JanusGraphDynamicTableSink(
                getOptions(helper.getOptions()),
                getJanusGraphProperties(catalogTable.getOptions()),
                primaryKeys,
                context.getPhysicalRowDataType());
    }

    private JanusGraphOptions getOptions(ReadableConfig config) {
        return new JanusGraphOptions.Builder()
                .setFactory(config.get(FACTORY))
                .setHosts(config.get(HOSTS))
                .setPort(config.get(PORT))
                .setBackendType(config.get(BACKEND_TYPE))
                .setUsername(config.get(USERNAME))
                .setPassword(config.get(PASSWORD))
                .setTableName(config.get(TABLE_NAME))
                .setTableType(config.get(TABLE_TYPE))
                .setBatchSize(config.get(SINK_BATCH_SIZE))
                .setFlushInterval(config.get(SINK_FLUSH_INTERVAL))
                .setMaxRetries(config.get(SINK_MAX_RETRIES))
                .setParallelism(config.get(SINK_PARALLELISM))
                .setNonUpdateColumns(config.get(SINK_NON_UPDATE_COLUMNS))
                .build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(HOSTS);
        requiredOptions.add(PORT);
        requiredOptions.add(BACKEND_TYPE);
        requiredOptions.add(TABLE_NAME);
        requiredOptions.add(TABLE_TYPE);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(FACTORY);
        optionalOptions.add(USERNAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(SINK_PARALLELISM);
        optionalOptions.add(SINK_BATCH_SIZE);
        optionalOptions.add(SINK_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(SINK_NON_UPDATE_COLUMNS);
        return optionalOptions;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(SINK_BATCH_SIZE);
        optionalOptions.add(SINK_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(SINK_NON_UPDATE_COLUMNS);
        return optionalOptions;
    }
}

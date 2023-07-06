package org.apache.flink.connector.janusgraph.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.janusgraph.options.UpdateNotFoundStrategy;
import org.apache.flink.connector.janusgraph.options.WriteMode;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;

/** JanusGraph config options. */
public class JanusGraphConfigOptions {

    public static final ConfigOption<String> FACTORY =
            ConfigOptions.key(JanusGraphConfig.FACTORY)
                    .stringType()
                    .defaultValue("org.janusgraph.core.JanusGraphFactory")
                    .withDescription(
                            "The Factory for creating a JanusGraph instance, equal to JanusGraph config `gremlin.graph`.");

    public static final ConfigOption<String> HOSTS =
            ConfigOptions.key(JanusGraphConfig.HOSTS)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A comma-separated list of storage backend servers, equal to JanusGraph config `storage.hostname`.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key(JanusGraphConfig.PORT)
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The port on which to connect to storage backend servers, equal to JanusGraph config `storage.port`.");

    public static final ConfigOption<BackendType> BACKEND_TYPE =
            ConfigOptions.key(JanusGraphConfig.BACKEND_TYPE)
                    .enumType(BackendType.class)
                    .defaultValue(BackendType.HBASE)
                    .withDescription("Type of storage backend, currently only supports `hbase`.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key(JanusGraphConfig.USERNAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Username to authenticate against storage backend, equal to JanusGraph config `storage.username`.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key(JanusGraphConfig.PASSWORD)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password to authenticate against storage backend, equal to JanusGraph config `storage.password`.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key(JanusGraphConfig.TABLE_NAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The JanusGraph table name, currently equal to JanusGraph config `storage.hbase.table`."
                                    + " TODO: Need to adopt to different storage backends.");

    public static final ConfigOption<TableType> TABLE_TYPE =
            ConfigOptions.key(JanusGraphConfig.TABLE_TYPE)
                    .enumType(TableType.class)
                    .noDefaultValue()
                    .withDescription("The type of current table, available: vertex, edge.");

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    public static final ConfigOption<WriteMode> SINK_MODE =
            ConfigOptions.key(JanusGraphConfig.SINK_MODE)
                    .enumType(WriteMode.class)
                    .defaultValue(WriteMode.APPEND)
                    .withDescription(
                            "Write mode for sink, available: append, upsert. The default value is append.");

    public static final ConfigOption<Integer> SINK_BATCH_SIZE =
            ConfigOptions.key(JanusGraphConfig.SINK_BATCH_SIZE)
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The max flush size, over this number of records, will flush data. The default value is 1000.");

    public static final ConfigOption<Duration> SINK_FLUSH_INTERVAL =
            ConfigOptions.key(JanusGraphConfig.SINK_FLUSH_INTERVAL)
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1L))
                    .withDescription(
                            "The flush interval mills, over this time, asynchronous threads will flush data. The default value is 1s.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key(JanusGraphConfig.SINK_MAX_RETRIES)
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The max retry times if writing records to JanusGraph failed.");

    public static final ConfigOption<String> SINK_NON_UPDATE_COLUMNS =
            ConfigOptions.key(JanusGraphConfig.SINK_NON_UPDATE_COLUMNS)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Comma separated list of columns that are not allowed to be updated.");

    public static final ConfigOption<UpdateNotFoundStrategy> SINK_UPDATE_NOT_FOUND_STRATEGY =
            ConfigOptions.key(JanusGraphConfig.SINK_UPDATE_NOT_FOUND_STRATEGY)
                    .enumType(UpdateNotFoundStrategy.class)
                    .defaultValue(UpdateNotFoundStrategy.FAIL)
                    .withDescription(
                            "The strategy when updating elements not found in JanusGraph, available: fail, ignore, insert.");
}

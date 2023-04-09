package org.apache.flink.connector.janusgraph.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;

/** clickhouse config options. */
public class JanusGraphConfigOptions {

    public static final ConfigOption<String> FACTORY =
            ConfigOptions.key(JanusGraphConfig.FACTORY)
                    .stringType()
                    .defaultValue("org.janusgraph.core.JanusGraphFactory")
                    .withDescription("The ClickHouse url in format `clickhouse://<host>:<port>`.");

    public static final ConfigOption<String> HOSTS =
            ConfigOptions.key(JanusGraphConfig.HOSTS)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Directly read/write local tables in case of distributed table engine.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key(JanusGraphConfig.PORT)
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Convert a record of type UPDATE_AFTER to update/insert statement or just discard it, available: update, insert, discard."
                                    + " Additional: `table.exec.sink.upsert-materialize`, `org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializer`");

    public static final ConfigOption<BackendType> BACKEND_TYPE =
            ConfigOptions.key(JanusGraphConfig.BACKEND_TYPE)
                    .enumType(BackendType.class)
                    .noDefaultValue()
                    .withDescription(
                            "Sharding strategy, available: balanced, hash, shuffle, value.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key(JanusGraphConfig.USERNAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The ClickHouse username.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key(JanusGraphConfig.PASSWORD)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The ClickHouse password.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key(JanusGraphConfig.TABLE_NAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The ClickHouse table name.");

    public static final ConfigOption<TableType> TABLE_TYPE =
            ConfigOptions.key(JanusGraphConfig.TABLE_TYPE)
                    .enumType(TableType.class)
                    .noDefaultValue()
                    .withDescription("The ClickHouse database name. Default to `default`.");

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

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
                    .withDescription("The max retry times if writing records to database failed.");
}

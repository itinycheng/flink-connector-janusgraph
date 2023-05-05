# Flink JanusGraph Connector

This project aims to use Flink to operate JanusGraph(Hbase Backend) simply and efficiently.

## How To Use

```SQL
-- register a vertex table in FlinkSQL
CREATE TABLE t_jg_vertex (
    `id` ROW <`label` STRING, `ident` STRING>, -- identify a unique vertex
    `label` STRING,                 -- graph's label
    `value` STRING,                 -- graph's value property 
    `create_at` TIMESTAMP_LTZ(3),   -- graph's create_at property 
    `update_at` TIMESTAMP_LTZ(3)    -- graph's update_at property 
) WITH (
    'connector' = 'janusgraph',
    'hosts' = '0.0.0.0,0.0.0.0',
    'port' = '2181',
    'backend-type' = 'hbase',
    'table-name' = 'jg_table',
    'table-type' = 'vertex',
    'sink.flush-interval' = '10s',
    'sink.batch-size' = '2000',
    'sink.non-update-columns' = 'create_at,..',
    'sink.parallelism' = '2',
    ...
);

-- register a edge table in FlinkSQL
CREATE TABLE t_jg_edge (
    `id` ROW<label STRING,..>, -- id + in_v + out_v can identify a unique edge
    `in_v` ROW<label STRING,..>,
    `out_v` ROW<label STRING,..>,
    `label` STRING,
    `value` STRING,
) WITH (
    'connector' = 'janusgraph',
    'hosts' = '0.0.0.0,0.0.0.0',
    'port' = '2181',
    'backend-type' = 'hbase',
    'table-name' = 'jg_table',
    'table-type' = 'edge',
    ...
)

-- 1. id, label, in_v, out_v are internally defined columns.
-- 2. Type of id, in_v, out_v can be BIGINT or ROW.
```

## Options

| Option                  | Required | Default                               | Type    | Description                                                                                         |
|:------------------------|:---------|:--------------------------------------|:--------|:----------------------------------------------------------------------------------------------------|
| connector               | true     | none                                  | String  | Specify which connector to use, here should be 'janusgraph'.                                        |
| factory                 | false    | org.janusgraph.core.JanusGraphFactory | String  | The Factory for creating a JanusGraph instance, equal to JanusGraph config `gremlin.graph`          |
| hosts                   | true     | none                                  | String  | A comma-separated list of storage backend servers, equal to JanusGraph config `storage.hostname`.   |
| port                    | true     | none                                  | Integer | The port on which to connect to storage backend servers, equal to JanusGraph config `storage.port`. |
| backend-type            | false    | hbase                                 | String  | Type of storage backend, currently only supports `hbase`.                                           |
| table-name              | true     | none                                  | String  | The JanusGraph table name, currently equal to JanusGraph config `storage.hbase.table`.              |
| table-type              | true     | none                                  | String  | The type of current table, available: vertex, edge.                                                 |
| username                | false    | none                                  | String  | Username to authenticate against storage backend, equal to JanusGraph config `storage.username`.    |
| password                | false    | none                                  | String  | Password to authenticate against storage backend, equal to JanusGraph config `storage.password`.    |
| sink.parallelism        | false    | none                                  | Integer | Defines a custom parallelism for the sink.                                                          |
| sink.batch-size         | false    | 1000                                  | Integer | The max flush size, over this number of records, will flush data.                                   |
| sink.flush-interval     | false    | 1s                                    | Integer | The flush interval mills, over this time, asynchronous threads will flush data.                     |
| sink.max-retries        | false    | 3                                     | Integer | The max retry times if writing records to JanusGraph failed.                                        |
| sink.non-update-columns | false    | none                                  | String  | A comma-separated list of columns that are not allowed to be updated.                               |
| properties.*            | optional | none                                  | String  | This can set and pass JanusGraph configurations.                                                    |  

## Data Type Mapping

| Flink Type                              | JanusGraph Type                         |
|:----------------------------------------|:----------------------------------------|
| CHAR / VARCHAR / STRING                 | String                                  |
| BOOLEAN                                 | Boolean                                 |
| BINARY / VARBINARY / BYTES              | byte[]                                  |
| TINYINT                                 | Byte                                    |
| SMALLINT                                | Short                                   |
| INTEGER / INTERVAL_YEAR_MONTH           | Integer                                 |
| BIGINT / INTERVAL_DAY_TIME              | Long                                    |
| FLOAT                                   | Float                                   |
| DOUBLE                                  | Double                                  |
| DATE / TIME / TIMESTAMP / TIMESTAMP_LTZ | Date                                    |
| ARRAY                                   | Array                                   |
| MAP                                     | Map                                     |
| ROW                                     | Supported by columns of id, in_v, out_v |
| DECIMAL                                 | Not supported                           |
| MULTISET                                | Not supported                           |
| RAW                                     | Not supported                           |

package org.apache.flink.connector.janusgraph.config;

/** JanusGraph config properties. */
public class JanusGraphConfig {

    public static final String IDENTIFIER = "janusgraph";

    public static final String PROPERTIES_PREFIX = "properties.";

    public static final String FACTORY = "factory";

    public static final String HOSTS = "hosts";

    public static final String PORT = "port";

    public static final String BACKEND_TYPE = "backend-type";

    public static final String TABLE_NAME = "table-name";

    public static final String TABLE_TYPE = "table-type";

    public static final String USERNAME = "username";

    public static final String PASSWORD = "password";

    public static final String SINK_BATCH_SIZE = "sink.batch-size";

    public static final String SINK_FLUSH_INTERVAL = "sink.flush-interval";

    public static final String SINK_MAX_RETRIES = "sink.max-retries";

    public static final String KEYWORD_LABEL = "label";

    public static final String KEYWORD_V_ID = "v_id";

    public static final String KEYWORD_FROM_V_ID = "from_v_id";

    public static final String KEYWORD_TO_V_ID = "to_v_id";
}

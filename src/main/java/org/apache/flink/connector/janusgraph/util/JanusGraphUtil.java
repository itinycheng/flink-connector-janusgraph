package org.apache.flink.connector.janusgraph.util;

import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.janusgraph.config.JanusGraphConfig.PROPERTIES_PREFIX;

/** Utils. */
public class JanusGraphUtil {
    public static Properties getJanusGraphProperties(Map<String, String> tableOptions) {
        final Properties properties = new Properties();

        int index = PROPERTIES_PREFIX.length();
        tableOptions.keySet().stream()
                .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                .forEach(
                        key -> {
                            final String value = tableOptions.get(key);
                            final String subKey = key.substring(index);
                            properties.setProperty(subKey, value);
                        });
        return properties;
    }

    public static ZoneId getFlinkZoneId() {
        return ZoneId.systemDefault();
    }
}

package org.apache.flink.connector.janusgraph.internal.connection;

import org.apache.commons.configuration2.Configuration;

import javax.annotation.Nonnull;

import java.util.Iterator;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Compare two configurations. */
public class SameConfiguration {

    private final Configuration configuration;

    public SameConfiguration(@Nonnull Configuration configuration) {
        this.configuration = checkNotNull(configuration);
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public Object getProperty(String key) {
        return configuration.getProperty(key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof SameConfiguration)) {
            return false;
        }

        SameConfiguration that = (SameConfiguration) o;
        for (Iterator<String> it = configuration.getKeys(); it.hasNext(); ) {
            String key = it.next();
            if (!Objects.equals(this.getProperty(key), that.getProperty(key))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        for (Iterator<String> it = configuration.getKeys(); it.hasNext(); ) {
            String key = it.next();
            result = 31 * result + Objects.hashCode(this.getProperty(key));
        }
        return result;
    }
}

package org.apache.nifi.atlas.resolver;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public interface ClusterResolver {

    default Collection<ValidationResult> validate(final ValidationContext validationContext) {
        return Collections.emptySet();
    }

    /**
     * Implementation should be clear previous configurations if when this method is called again.
     * @param context passed from ReportingTask
     */
    void configure(PropertyContext context);

    // TODO: add default cluster name capability.
    default String fromHostname(String hostname) {
        return null;
    }

    /**
     * Resolve a cluster name from hints, such as Zookeeper Quorum, client port and znode path
     * @param hints
     * @return
     */
    default String fromHints(Map<String, String> hints) {
        return null;
    }

}

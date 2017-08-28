package org.apache.nifi.atlas.provenance;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.provenance.ProvenanceEventRecord;

import java.net.URI;
import java.net.URISyntaxException;

public interface NiFiProvenanceEventAnalyzer {

    /**
     * Utility method to parse a string uri silently.
     * @param uri uri to parse
     * @return parsed URI instance
     */
    default URI parseUri(String uri) {
        try {
            return new URI(uri);
        } catch (URISyntaxException e) {
            final String msg = String.format("Failed to parse uri %s due to %s", uri, e);
            throw new IllegalArgumentException(msg, e);
        }
    }

    DataSetRefs analyze(ProvenanceEventRecord event);

    void setClusterResolver(ClusterResolver clusterResolver);

    /**
     * Returns target component type pattern that this Analyzer supports.
     * Note that a component type of NiFi provenance event only has processor type name without package name.
     * @return A RegularExpression to match with a component type of a provenance event.
     */
    default String targetComponentTypePattern() {
        return null;
    }

    /**
     * Returns target transit URI pattern that this Analyzer supports.
     * @return A RegularExpression to match with a transit URI of a provenance event.
     */
    default String targetTransitUriPattern() {
        return null;
    }

    void setLogger(ComponentLog logger);

}

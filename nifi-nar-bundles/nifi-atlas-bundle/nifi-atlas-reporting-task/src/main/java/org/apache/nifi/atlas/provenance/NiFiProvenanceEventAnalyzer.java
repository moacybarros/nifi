package org.apache.nifi.atlas.provenance;

import org.apache.nifi.provenance.ProvenanceEventRecord;

/**
 * Responsible for analyzing NiFi provenance event data to generate Atlas DataSet reference.
 * Implementations of this interface should be thread safe.
 */
public interface NiFiProvenanceEventAnalyzer {

    DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event);

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

}

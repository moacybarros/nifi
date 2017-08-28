package org.apache.nifi.atlas.provenance;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.provenance.ProvenanceEventType;

import java.util.Collections;

public abstract class AbstractNiFiProvenanceEventAnalyzer implements NiFiProvenanceEventAnalyzer {

    protected ComponentLog logger;
    protected ClusterResolver clusterResolver;

    @Override
    public void setClusterResolver(ClusterResolver clusterResolver) {
        this.clusterResolver = clusterResolver;
    }

    @Override
    public void setLogger(ComponentLog logger) {
        this.logger = logger;
    }

    protected String toQualifiedName(String clusterName, String dataSetName) {
        return dataSetName + "@" + clusterName;
    }

    protected DataSetRefs singleDataSetRef(ProvenanceEventType eventType, Referenceable ref) {
        final DataSetRefs refs = new DataSetRefs();
        switch (eventType) {
            case FETCH:
            case SEND:
                refs.setOutputs(Collections.singleton(ref));
                break;
            case RECEIVE:
                refs.setInputs(Collections.singleton(ref));
                break;
        }

        return refs;
    }

}

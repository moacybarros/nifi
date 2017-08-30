package org.apache.nifi.atlas.provenance;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.resolver.ClusterResolvers;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.provenance.ProvenanceEventType;

import java.util.Collections;

public abstract class AbstractNiFiProvenanceEventAnalyzer implements NiFiProvenanceEventAnalyzer {

    protected ComponentLog logger;
    protected ClusterResolvers clusterResolvers;

    @Override
    public void setClusterResolvers(ClusterResolvers clusterResolvers) {
        this.clusterResolvers = clusterResolvers;
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
            case SEND:
                refs.setOutputs(Collections.singleton(ref));
                break;
            case FETCH:
            case RECEIVE:
                refs.setInputs(Collections.singleton(ref));
                break;
        }

        return refs;
    }

}

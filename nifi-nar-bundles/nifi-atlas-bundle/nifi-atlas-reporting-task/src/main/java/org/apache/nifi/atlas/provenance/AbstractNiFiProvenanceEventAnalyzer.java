package org.apache.nifi.atlas.provenance;

public abstract class AbstractNiFiProvenanceEventAnalyzer implements NiFiProvenanceEventAnalyzer {

    protected ClusterResolver clusterResolver;

    @Override
    public void setClusterResolver(ClusterResolver clusterResolver) {
        this.clusterResolver = clusterResolver;
    }

    protected String toQualifiedName(String clusterName, String dataSetName) {
        return dataSetName + "@" + clusterName;
    }

}

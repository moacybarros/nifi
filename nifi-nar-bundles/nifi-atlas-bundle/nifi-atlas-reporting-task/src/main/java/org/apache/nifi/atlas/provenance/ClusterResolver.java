package org.apache.nifi.atlas.provenance;

public interface ClusterResolver {

    default String toClusterName(String hostname) {
        return "hoge";
    }

}

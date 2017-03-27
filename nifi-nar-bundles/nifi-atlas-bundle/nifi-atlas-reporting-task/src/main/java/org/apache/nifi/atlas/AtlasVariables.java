package org.apache.nifi.atlas;

public class AtlasVariables {

    private String nifiUrl;
    private String atlasClusterName;

    public String getNifiUrl() {
        return nifiUrl;
    }

    public void setNifiUrl(String nifiUrl) {
        this.nifiUrl = nifiUrl;
    }

    public String getAtlasClusterName() {
        return atlasClusterName;
    }

    public void setAtlasClusterName(String atlasClusterName) {
        this.atlasClusterName = atlasClusterName;
    }
}

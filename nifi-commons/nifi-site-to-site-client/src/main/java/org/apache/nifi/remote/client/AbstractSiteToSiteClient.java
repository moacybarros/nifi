package org.apache.nifi.remote.client;

public abstract class AbstractSiteToSiteClient implements SiteToSiteClient {

    protected final SiteToSiteClientConfig config;

    public AbstractSiteToSiteClient(final SiteToSiteClientConfig config) {
        this.config = config;
    }

    @Override
    public SiteToSiteClientConfig getConfig() {
        return config;
    }
}
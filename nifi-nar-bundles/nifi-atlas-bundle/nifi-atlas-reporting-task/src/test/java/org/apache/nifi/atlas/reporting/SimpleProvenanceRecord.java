package org.apache.nifi.atlas.reporting;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleProvenanceRecord implements ProvenanceEventRecord {
    private long eventId;
    private String componentId;
    private String componentType;
    private String transitUri;
    private ProvenanceEventType eventType;
    private Map<String, String> attributes = new HashMap<>();

    public static SimpleProvenanceRecord pr(String componentId, String componentType, ProvenanceEventType eventType) {
        return pr(componentId, componentType, eventType, null);
    }
    public static SimpleProvenanceRecord pr(String componentId, String componentType, ProvenanceEventType eventType, String transitUri) {
        final SimpleProvenanceRecord pr = new SimpleProvenanceRecord();
        pr.componentId = componentId;
        pr.componentType = componentType;
        pr.transitUri = transitUri;
        pr.eventType = eventType;
        return pr;
    }

    public void setEventId(long eventId) {
        this.eventId = eventId;
    }

    @Override
    public String getComponentId() {
        return componentId;
    }

    @Override
    public String getComponentType() {
        return componentType;
    }

    @Override
    public String getTransitUri() {
        return transitUri;
    }

    @Override
    public ProvenanceEventType getEventType() {
        return eventType;
    }

    @Override
    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public long getEventId() {
        return eventId;
    }

    @Override
    public long getEventTime() {
        return 0;
    }

    @Override
    public long getFlowFileEntryDate() {
        return 0;
    }

    @Override
    public long getLineageStartDate() {
        return 0;
    }

    @Override
    public long getFileSize() {
        return 0;
    }

    @Override
    public Long getPreviousFileSize() {
        return null;
    }

    @Override
    public long getEventDuration() {
        return 0;
    }

    @Override
    public Map<String, String> getPreviousAttributes() {
        return null;
    }

    @Override
    public Map<String, String> getUpdatedAttributes() {
        return null;
    }

    @Override
    public String getSourceSystemFlowFileIdentifier() {
        return null;
    }

    @Override
    public String getFlowFileUuid() {
        return null;
    }

    @Override
    public List<String> getParentUuids() {
        return null;
    }

    @Override
    public List<String> getChildUuids() {
        return null;
    }

    @Override
    public String getAlternateIdentifierUri() {
        return null;
    }

    @Override
    public String getDetails() {
        return null;
    }

    @Override
    public String getRelationship() {
        return null;
    }

    @Override
    public String getSourceQueueIdentifier() {
        return null;
    }

    @Override
    public String getContentClaimSection() {
        return null;
    }

    @Override
    public String getPreviousContentClaimSection() {
        return null;
    }

    @Override
    public String getContentClaimContainer() {
        return null;
    }

    @Override
    public String getPreviousContentClaimContainer() {
        return null;
    }

    @Override
    public String getContentClaimIdentifier() {
        return null;
    }

    @Override
    public String getPreviousContentClaimIdentifier() {
        return null;
    }

    @Override
    public Long getContentClaimOffset() {
        return null;
    }

    @Override
    public Long getPreviousContentClaimOffset() {
        return null;
    }

    @Override
    public String getBestEventIdentifier() {
        return null;
    }
}

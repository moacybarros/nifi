package org.apache.nifi.atlas;

import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.DataSetRefs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_DESCRIPTION;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_INPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_URL;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW_PATH;

/**
 * This class is not thread-safe as it holds uncommitted notification messages within instance.
 * {@link #addDataSetRefs(DataSetRefs, Referenceable)} and {@link #commitMessages()} should be used serially from a single thread.
 */
public class NiFIAtlasHook extends AtlasHook {

    private static final String CONF_PREFIX = "atlas.hook.nifi.";
    private static final String HOOK_NUM_RETRIES = CONF_PREFIX + "numRetries";

    @Override
    protected String getNumberOfRetriesPropertyKey() {
        return HOOK_NUM_RETRIES;
    }


    private static final String NIFI_USER = "nifi";

    private final List<HookNotification.HookNotificationMessage> messages = new ArrayList<>();

    @SuppressWarnings("unchecked")
    private void addDataSetRefs(Set<Referenceable> dataSetRefs, Referenceable nifiFlowPath, String targetAttribute) {
        if (dataSetRefs != null && !dataSetRefs.isEmpty()) {
            for (Referenceable dataSetRef : dataSetRefs) {
                final HookNotification.EntityCreateRequest createDataSet = new HookNotification.EntityCreateRequest(NIFI_USER, dataSetRef);
                messages.add(createDataSet);
            }

            Object updatedRef = nifiFlowPath.get(targetAttribute);
            if (updatedRef == null) {
                updatedRef = new ArrayList(dataSetRefs);
            } else {
                ((Collection<Referenceable>) updatedRef).addAll(dataSetRefs);
            }
            nifiFlowPath.set(targetAttribute, updatedRef);
        }
    }

    public void addDataSetRefs(DataSetRefs dataSetRefs, Referenceable flowPathRef) {
        addDataSetRefs(dataSetRefs.getInputs(), flowPathRef, ATTR_INPUTS);
        addDataSetRefs(dataSetRefs.getOutputs(), flowPathRef, ATTR_OUTPUTS);
        // Here, EntityPartialUpdateRequest adds Process's inputs or outputs elements who does not exists in
        // the current nifi_flow_path entity stored in Atlas.
        messages.add(new HookNotification.EntityPartialUpdateRequest(NIFI_USER, TYPE_NIFI_FLOW_PATH,
                ATTR_QUALIFIED_NAME, (String) flowPathRef.get(ATTR_QUALIFIED_NAME), flowPathRef));
    }

    public void commitMessages() {
        try {
            notifyEntities(messages);
        } finally {
            messages.clear();
        }
    }
}

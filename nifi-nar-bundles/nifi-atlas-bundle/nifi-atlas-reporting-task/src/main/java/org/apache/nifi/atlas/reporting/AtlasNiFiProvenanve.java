package org.apache.nifi.atlas.reporting;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer.BATCH_SIZE;
import static org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer.START_POSITION;

// TODO: doc
@Stateful(scopes = Scope.LOCAL, description = "Stores the Reporting Task's last event Id so that on restart the task knows where it left off.")
@Restricted("Provides operator the ability send sensitive details contained in Provenance events to any external system.")
public class AtlasNiFiProvenanve extends AbstractReportingTask {

    private volatile ProvenanceEventConsumer consumer;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(START_POSITION);
        properties.add(BATCH_SIZE);
        return properties;
    }

    @OnScheduled
    public void onScheduled(final ConfigurationContext context) throws IOException {
        consumer = new ProvenanceEventConsumer();
        // TODO: pass context to initialize consumer?
        consumer.setStartPositionValue(context.getProperty(START_POSITION).getValue());
        consumer.setBatchSize(context.getProperty(BATCH_SIZE).asInteger());
        consumer.setLogger(getLogger());
        consumer.setScheduled(true);
    }

    @OnUnscheduled
    public void onUnscheduled() {
        consumer.setScheduled(false);
    }

    @Override
    public void onTrigger(ReportingContext context) {

        consumer.consumeEvents(context.getEventAccess(), context.getStateManager(), events -> {
            for (ProvenanceEventRecord event : events) {
                getLogger().warn("Received event: {}", new Object[]{event});
            }
        });
    }

}

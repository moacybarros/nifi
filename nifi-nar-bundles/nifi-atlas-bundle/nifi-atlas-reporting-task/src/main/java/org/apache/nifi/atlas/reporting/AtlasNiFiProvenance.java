package org.apache.nifi.atlas.reporting;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.atlas.NiFIAtlasHook;
import org.apache.nifi.atlas.NiFiFlow;
import org.apache.nifi.atlas.NiFiFlowPath;
import org.apache.nifi.atlas.resolver.ClusterResolver;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzerFactory;
import org.apache.nifi.atlas.resolver.ClusterResolvers;
import org.apache.nifi.atlas.resolver.RegexClusterResolver;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW_PATH;
import static org.apache.nifi.provenance.ProvenanceEventType.FETCH;
import static org.apache.nifi.provenance.ProvenanceEventType.RECEIVE;
import static org.apache.nifi.provenance.ProvenanceEventType.SEND;
import static org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer.BATCH_SIZE;
import static org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer.START_POSITION;

// TODO: doc
// TODO: merge this to AtlasNiFiFlowLineage
@Stateful(scopes = Scope.LOCAL, description = "Stores the Reporting Task's last event Id so that on restart the task knows where it left off.")
@Restricted("Provides operator the ability send sensitive details contained in Provenance events to any external system.")
public class AtlasNiFiProvenance extends AbstractReportingTask {

    private volatile ProvenanceEventConsumer consumer;
    private volatile ClusterResolvers clusterResolvers;
    private volatile NiFIAtlasHook nifiAtlasHook;


    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        return new RegexClusterResolver().validate(validationContext);
    }

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
        consumer.setStartPositionValue(context.getProperty(START_POSITION).getValue());
        consumer.setBatchSize(context.getProperty(BATCH_SIZE).asInteger());
        consumer.addTargetEventType(FETCH, RECEIVE, SEND);
        consumer.setLogger(getLogger());
        consumer.setScheduled(true);

        final Set<ClusterResolver> loadedClusterResolvers = new LinkedHashSet<>();
        final ServiceLoader<ClusterResolver> clusterResolverLoader = ServiceLoader.load(ClusterResolver.class);
        clusterResolverLoader.forEach(resolver -> {
            resolver.configure(context);
            loadedClusterResolvers.add(resolver);
        });
        clusterResolvers = new ClusterResolvers(Collections.unmodifiableSet(loadedClusterResolvers), null);

        nifiAtlasHook = new NiFIAtlasHook();
    }

    @OnUnscheduled
    public void onUnscheduled() {
        consumer.setScheduled(false);
    }

    @Override
    public void onTrigger(ReportingContext context) {

        // TODO: somehow expose constructed NiFiFlow object
        final NiFiFlow nifiFlow = null;
        consumer.consumeEvents(context.getEventAccess(), context.getStateManager(), events -> {
            for (ProvenanceEventRecord event : events) {
                final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(event.getComponentType(), event.getTransitUri());
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("Analyzer {} is found for event: {}", new Object[]{analyzer, event});
                }
                if (analyzer == null) {
                    continue;
                }
                analyzer.setLogger(getLogger());
                analyzer.setClusterResolvers(clusterResolvers);
                final DataSetRefs refs = analyzer.analyze(event);
                if (refs == null || (refs.isEmpty())) {
                    continue;
                }

                // create reference to NiFi flow path.
                final NiFiFlowPath flowPath = nifiFlow.findPath(event.getComponentId());
                if (flowPath == null) {
                    getLogger().warn("FlowPath for {} was not found.", new Object[]{event.getComponentId()});
                }

                final Referenceable nifiFlowPath = null;
                final Referenceable ref = new Referenceable(TYPE_NIFI_FLOW_PATH);
                ref.set(ATTR_NAME, flowPath.getName());
                ref.set(ATTR_QUALIFIED_NAME, flowPath.getId());

                nifiAtlasHook.addDataSetRefs(refs, nifiFlowPath);
                nifiAtlasHook.commitMessages();
            }
        });
    }

}

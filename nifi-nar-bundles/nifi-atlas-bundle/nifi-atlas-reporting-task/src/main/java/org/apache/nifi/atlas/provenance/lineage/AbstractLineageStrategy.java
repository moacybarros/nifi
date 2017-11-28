/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.atlas.provenance.lineage;

import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.NiFiFlow;
import org.apache.nifi.atlas.NiFiFlowPath;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzerFactory;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_INPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_URL;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW_PATH;

public abstract class AbstractLineageStrategy implements LineageStrategy {

    private static final String NIFI_USER = "nifi";

    protected Logger logger = LoggerFactory.getLogger(getClass());
    private LineageContext lineageContext;

    public void setLineageContext(LineageContext lineageContext) {
        this.lineageContext = lineageContext;
    }

    protected DataSetRefs executeAnalyzer(AnalysisContext analysisContext, ProvenanceEventRecord event) {
        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(event.getComponentType(), event.getTransitUri(), event.getEventType());
        if (analyzer == null) {
            return null;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Analyzer {} is found for event: {}", analyzer, event);
        }
        return analyzer.analyze(analysisContext, event);
    }

    protected void addDataSetRefs(NiFiFlow nifiFlow, DataSetRefs refs) {

        final Set<NiFiFlowPath> flowPaths = refs.getComponentIds().stream()
                .map(componentId -> {
                    final NiFiFlowPath flowPath = nifiFlow.findPath(componentId);
                    if (flowPath == null) {
                        logger.warn("FlowPath for {} was not found.", componentId);
                    }
                    return flowPath;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        addDataSetRefs(nifiFlow, flowPaths, refs);


    }

    protected void addDataSetRefs(NiFiFlow nifiFlow, Set<NiFiFlowPath> flowPaths, DataSetRefs refs) {
        // create reference to NiFi flow path.
        final Referenceable flowRef = toReferenceable(nifiFlow);

        for (NiFiFlowPath flowPath : flowPaths) {
            final Referenceable flowPathRef = toReferenceable(flowPath, flowRef);
            addDataSetRefs(refs, flowPathRef);
        }
    }

    protected Referenceable toReferenceable(NiFiFlow nifiFlow) {
        final Referenceable flowRef = new Referenceable(TYPE_NIFI_FLOW);
        flowRef.set(ATTR_NAME, nifiFlow.getFlowName());
        flowRef.set(ATTR_QUALIFIED_NAME, nifiFlow.getId().getUniqueAttributes().get(ATTR_QUALIFIED_NAME));
        flowRef.set(ATTR_URL, nifiFlow.getUrl());
        return flowRef;
    }

    protected Referenceable toReferenceable(NiFiFlowPath flowPath, NiFiFlow nifiFlow) {
        return toReferenceable(flowPath, toReferenceable(nifiFlow));
    }

    protected Referenceable toReferenceable(NiFiFlowPath flowPath, Referenceable flowRef) {
        final Referenceable flowPathRef = new Referenceable(TYPE_NIFI_FLOW_PATH);
        flowPathRef.set(ATTR_NAME, flowPath.getName());
        flowPathRef.set(ATTR_QUALIFIED_NAME, flowPath.getId());
        flowPathRef.set(ATTR_NIFI_FLOW, flowRef);
        flowPathRef.set(ATTR_URL, flowPath.createDeepLinkURL((String) flowRef.get(ATTR_URL)));
        return flowPathRef;
    }

    // TODO: Refactor lineageContext and NiFiAtlasHook.


    protected void createEntity(Referenceable ... entities) {
        final HookNotification.EntityCreateRequest msg = new HookNotification.EntityCreateRequest(NIFI_USER, entities);
        lineageContext.addMessage(msg);
    }

    @SuppressWarnings("unchecked")
    protected void addDataSetRefs(Set<Referenceable> dataSetRefs, Referenceable nifiFlowPath, String targetAttribute) {
        if (dataSetRefs != null && !dataSetRefs.isEmpty()) {
            for (Referenceable dataSetRef : dataSetRefs) {
                final HookNotification.EntityCreateRequest createDataSet = new HookNotification.EntityCreateRequest(NIFI_USER, dataSetRef);
                lineageContext.addMessage(createDataSet);
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

    protected void addDataSetRefs(DataSetRefs dataSetRefs, Referenceable flowPathRef) {
        addDataSetRefs(dataSetRefs.getInputs(), flowPathRef, ATTR_INPUTS);
        addDataSetRefs(dataSetRefs.getOutputs(), flowPathRef, ATTR_OUTPUTS);
        // Here, EntityPartialUpdateRequest adds Process's inputs or outputs elements who does not exists in
        // the current nifi_flow_path entity stored in Atlas.
        lineageContext.addMessage(new HookNotification.EntityPartialUpdateRequest(NIFI_USER, TYPE_NIFI_FLOW_PATH,
                ATTR_QUALIFIED_NAME, (String) flowPathRef.get(ATTR_QUALIFIED_NAME), flowPathRef));
    }


}

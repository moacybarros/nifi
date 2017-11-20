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
package org.apache.nifi.atlas;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.reporting.ReportingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_DESCRIPTION;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_INPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_OUTPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;
import static org.apache.nifi.atlas.reporting.AtlasNiFiFlowLineage.ATLAS_NIFI_URL;

public class NiFiFlowAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(NiFiFlowAnalyzer.class);

    public NiFiFlow analyzeProcessGroup(ReportingContext context) throws IOException {
        final ProcessGroupStatus rootProcessGroup = context.getEventAccess().getGroupStatus("root");

        final String flowName = rootProcessGroup.getName();

        final String nifiUrl = context.getProperty(ATLAS_NIFI_URL).evaluateAttributeExpressions().getValue();
        final NiFiFlow nifiFlow = new NiFiFlow(flowName, rootProcessGroup.getId(), nifiUrl);

        analyzeProcessGroup(rootProcessGroup, nifiFlow);

        analyzeRootGroupPorts(nifiFlow, rootProcessGroup);

        return nifiFlow;
    }

    private void analyzeRootGroupPorts(NiFiFlow nifiFlow, ProcessGroupStatus rootProcessGroup) {
        BiConsumer<PortStatus, Boolean> portEntityCreator = (port, isInput) -> {
            final String typeName = isInput ? TYPE_NIFI_INPUT_PORT : TYPE_NIFI_OUTPUT_PORT;

            final AtlasEntity entity = new AtlasEntity(typeName);
            final String portName = port.getName();

            entity.setAttribute(ATTR_NIFI_FLOW, nifiFlow.getId());
            entity.setAttribute(ATTR_NAME, portName);
            entity.setAttribute(ATTR_QUALIFIED_NAME, port.getId());

            final AtlasObjectId portId = new AtlasObjectId(typeName, ATTR_QUALIFIED_NAME, port.getId());
            final Map<AtlasObjectId, AtlasEntity> ports = isInput ? nifiFlow.getRootInputPortEntities() : nifiFlow.getRootOutputPortEntities();
            ports.put(portId, entity);

            if (isInput) {
                nifiFlow.addRootInputPort(port);
            } else {
                nifiFlow.addRootOutputPort(port);
            }
        };

        rootProcessGroup.getInputPortStatus().forEach(port -> portEntityCreator.accept(port, true));
        rootProcessGroup.getOutputPortStatus().forEach(port -> portEntityCreator.accept(port, false));
    }

    private void analyzeProcessGroup(final ProcessGroupStatus processGroupStatus, final NiFiFlow nifiFlow) throws IOException {

        processGroupStatus.getConnectionStatus().forEach(c -> nifiFlow.addConnection(c));
        processGroupStatus.getProcessorStatus().forEach(p -> nifiFlow.addProcessor(p));
        processGroupStatus.getRemoteProcessGroupStatus().forEach(r -> nifiFlow.addRemoteProcessGroup(r));
        processGroupStatus.getInputPortStatus().forEach(p -> nifiFlow.addInputPort(p));
        processGroupStatus.getOutputPortStatus().forEach(p -> nifiFlow.addOutputPort(p));

        // Analyze child ProcessGroups recursively.
        for (ProcessGroupStatus child : processGroupStatus.getProcessGroupStatus()) {
            analyzeProcessGroup(child, nifiFlow);
        }

    }

    private List<String> getIncomingProcessorsIds(NiFiFlow nifiFlow, List<ConnectionStatus> incomingConnections) {
        if (incomingConnections == null) {
            return Collections.emptyList();
        }

        final List<String> ids = new ArrayList<>();

        incomingConnections.forEach(c -> {
            // Ignore self relationship.
            final String sourceId = c.getSourceId();
            if (!sourceId.equals(c.getDestinationId())) {
                if (nifiFlow.isProcessor(sourceId)) {
                    ids.add(sourceId);
                } else {
                    ids.addAll(getIncomingProcessorsIds(nifiFlow, nifiFlow.getIncomingRelationShips(sourceId)));
                }
            }
        });

        return ids;
    }

    private List<String> getNextProcessors(NiFiFlow nifiFlow, NiFiFlowPath path, String componentId) {
        final List<ConnectionStatus> outs = nifiFlow.getOutgoingRelationShips(componentId);
        if (outs == null || outs.isEmpty()) {
            return Collections.emptyList();
        }

        final List<String> nextProcessors = new ArrayList<>();
        for (ConnectionStatus out : outs) {
            final String destinationId = out.getDestinationId();
            if (path.getProcessorIds().contains(destinationId)) {
                // If the connection is pointing back to current path, then skip it to avoid loop.
                continue;
            }

            if (nifiFlow.isProcessor(destinationId)) {
                nextProcessors.add(destinationId);
            } else {
                nextProcessors.addAll(getNextProcessors(nifiFlow, path, destinationId));
            }
        }
        return nextProcessors;
    }

    private void traverse(NiFiFlow nifiFlow, List<NiFiFlowPath> paths, NiFiFlowPath path, String pid) {

        // If the pid is RootGroupPort of the same NiFi instance, then stop traversing to create separate self S2S path.
        // E.g InputPort -> MergeContent, GenerateFlowFile -> InputPort.
        if (nifiFlow.isRootInputPort(pid) || nifiFlow.isRootOutputPort(pid)) {
            return;
        }

        // Add known inputs/outputs to/from this processor, such as RootGroupIn/Output port
        if (nifiFlow.getProcessors().containsKey(pid)) {
            path.addProcessor(pid);

            if (nifiFlow.getIncomingRelationShips(pid) != null) {
                nifiFlow.getIncomingRelationShips(pid).stream().forEach(c -> {
                    final String sourceId = c.getSourceId();
                    if (nifiFlow.isRootInputPort(sourceId)) {
                        // This processor has incoming connection from a RootGroupInputPort.
                        final AtlasObjectId portId = new AtlasObjectId(TYPE_NIFI_INPUT_PORT, ATTR_QUALIFIED_NAME, sourceId);
                        path.getInputs().add(portId);
                    }
                });
            }

            if (nifiFlow.getOutgoingRelationShips(pid) != null) {
                nifiFlow.getOutgoingRelationShips(pid).stream().forEach(c -> {
                    final String destinationId = c.getDestinationId();
                    if (nifiFlow.isRootOutputPort(destinationId)) {
                        // This processor has outgoing connection to a RootGroupOutputPort.
                        final AtlasObjectId portId = new AtlasObjectId(TYPE_NIFI_OUTPUT_PORT, ATTR_QUALIFIED_NAME, destinationId);
                        path.getOutputs().add(portId);
                    }
                });
            }

        }

        final List<ConnectionStatus> outs = nifiFlow.getOutgoingRelationShips(pid);
        if (outs == null || outs.isEmpty()) {
            return;
        }

        // Analyze destination processors.
        final List<String> nextProcessors = getNextProcessors(nifiFlow, path, pid);
        nextProcessors.forEach(destPid -> {
            if (path.getProcessorIds().contains(destPid)) {
                // Avoid loop to it self.
                return;
            }

            // If destination has more than one inputs, or there are multiple destinations, it is an independent flow path.
            final boolean createJointPoint = nextProcessors.size() > 1 || getIncomingProcessorsIds(nifiFlow, nifiFlow.getIncomingRelationShips(destPid)).size() > 1;

            if (createJointPoint) {
                final NiFiFlowPath newJointPoint = new NiFiFlowPath(destPid);

                final boolean exists = paths.contains(newJointPoint);
                final NiFiFlowPath jointPoint = exists ? paths.stream()
                        .filter(p -> p.equals(newJointPoint)).findFirst().get() : newJointPoint;

                // Link together.
                path.getOutgoingPaths().add(jointPoint);
                jointPoint.getIncomingPaths().add(path);

                if (exists) {
                    // Link existing incoming queue of the joint point.
                    path.getOutputs().add(jointPoint.getInputs().iterator().next());

                } else {
                    // Add jointPoint only if it doesn't exist, to avoid adding the same jointPoint again.
                    paths.add(jointPoint);

                    // Create an input queue DataSet because Atlas doesn't show lineage if it doesn't have in and out.
                    // This DataSet is also useful to link flowPaths together on Atlas lineage graph.
                    final AtlasObjectId queueId = new AtlasObjectId(TYPE_NIFI_QUEUE, ATTR_QUALIFIED_NAME, destPid);

                    final AtlasEntity queue = new AtlasEntity(TYPE_NIFI_QUEUE);
                    queue.setAttribute(ATTR_NIFI_FLOW, nifiFlow.getId());
                    queue.setAttribute(ATTR_QUALIFIED_NAME, destPid);
                    queue.setAttribute(ATTR_NAME, "queue");
                    queue.setAttribute(ATTR_DESCRIPTION, "Input queue for " + destPid);

                    nifiFlow.getQueues().put(queueId, queue);
                    newJointPoint.getInputs().add(queueId);
                    path.getOutputs().add(queueId);

                    // Start traversing as a new joint point.
                    traverse(nifiFlow, paths, jointPoint, destPid);
                }

            } else {
                // Normal relation, continue digging.
                traverse(nifiFlow, paths, path, destPid);
            }

        });
    }

    private boolean isHeadProcessor(NiFiFlow nifiFlow, List<ConnectionStatus> ins) {
        if (ins == null || ins.isEmpty()) {
            return true;
        }
        return ins.stream().allMatch(
                in -> {
                    // If it has incoming relationship from other processor, then return false.
                    final String sourceId = in.getSourceId();
                    if (nifiFlow.isProcessor(sourceId)) {
                        return false;
                    }
                    // Terminate traversing if it only has incoming relationships from RootGroupPort,
                    // or non-processor such as funnel so that it can be an individual flow path.
                    final List<ConnectionStatus> incomingRelationShips = nifiFlow.getIncomingRelationShips(sourceId);
                    final boolean noFurtherInputs = incomingRelationShips == null || incomingRelationShips.isEmpty();
                    return nifiFlow.isRootInputPort(sourceId) || nifiFlow.isRootOutputPort(sourceId) || noFurtherInputs
                            // Check next level.
                            || isHeadProcessor(nifiFlow, incomingRelationShips);
                }
        );
    }

    public void analyzePaths(NiFiFlow nifiFlow) {
        final List<NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        // Root path is used to link DataSets those are not connected to actual flow_path,
        // such as a flow receiving from a Remote Output Port then sending to a Remote Input Port directly.
        final String rootProcessGroupId = nifiFlow.getRootProcessGroupId();
        final NiFiFlowPath rootPath = new NiFiFlowPath(rootProcessGroupId);
        rootPath.setGroupId(rootProcessGroupId);
        paths.add(rootPath);

        // Now let's break it into flow paths.
        final Map<String, ProcessorStatus> processors = nifiFlow.getProcessors();
        final Set<String> headProcessors = processors.keySet().stream()
                .filter(pid -> {
                    final List<ConnectionStatus> ins = nifiFlow.getIncomingRelationShips(pid);
                    return isHeadProcessor(nifiFlow, ins);
                })
                .collect(Collectors.toSet());

        headProcessors.forEach(startPid -> {
            // By using the startPid as its qualifiedName, it's guaranteed that
            // the same path will end up being the same Atlas entity.
            // However, if the first processor is replaced by another,
            // the flow path will have a different id, and the old path is logically deleted.
            final NiFiFlowPath path = new NiFiFlowPath(startPid);
            paths.add(path);
            traverse(nifiFlow, paths, path, startPid);
        });

        paths.forEach(path -> {
            if (processors.containsKey(path.getId())) {
                final ProcessorStatus processor = processors.get(path.getId());
                path.setGroupId(processor.getGroupId());
            }
        });
    }

}

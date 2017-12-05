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
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_DESCRIPTION;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_INPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_OUTPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;

public class NiFiFlow {

    private static final Logger logger = LoggerFactory.getLogger(NiFiFlow.class);

    private final String rootProcessGroupId;
    private String flowName;
    private String clusterName;
    private String url;
    private String atlasGuid;
    private AtlasEntity exEntity;
    private AtlasObjectId atlasObjectId;
    private String description;

    private final Map<String, NiFiFlowPath> flowPaths = new HashMap<>();
    private final Map<String, ProcessorStatus> processors = new HashMap<>();
    private final Map<String, RemoteProcessGroupStatus> remoteProcessGroups = new HashMap<>();
    private final Map<String, List<ConnectionStatus>> incomingRelationShips = new HashMap<>();
    private final Map<String, List<ConnectionStatus>> outGoingRelationShips = new HashMap<>();

    private final Map<AtlasObjectId, AtlasEntity> queues = new HashMap<>();
    // Any Ports.
    private final Map<String, PortStatus> inputPorts = new HashMap<>();
    private final Map<String, PortStatus> outputPorts = new HashMap<>();
    // Root Group Ports.
    private final Map<String, PortStatus> rootInputPorts = new HashMap<>();
    private final Map<String, PortStatus> rootOutputPorts = new HashMap<>();
    // Root Group Ports Entity.
    private final Map<AtlasObjectId, AtlasEntity> rootInputPortEntities = new HashMap<>();
    private final Map<AtlasObjectId, AtlasEntity> rootOutputPortEntities = new HashMap<>();


    public NiFiFlow(String rootProcessGroupId) {
        this.rootProcessGroupId = rootProcessGroupId;
    }

    public AtlasObjectId getAtlasObjectId() {
        return atlasObjectId;
    }

    public String getRootProcessGroupId() {
        return rootProcessGroupId;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
        atlasObjectId = createAtlasObjectId();
    }

    private AtlasObjectId createAtlasObjectId() {
        return new AtlasObjectId(atlasGuid, TYPE_NIFI_FLOW, Collections.singletonMap(ATTR_QUALIFIED_NAME, getQualifiedName()));
    }

    public AtlasEntity getExEntity() {
        return exEntity;
    }

    public void setExEntity(AtlasEntity exEntity) {
        this.exEntity = exEntity;
        this.setAtlasGuid(exEntity.getGuid());
    }

    public String getAtlasGuid() {
        return atlasGuid;
    }

    public void setAtlasGuid(String atlasGuid) {
        this.atlasGuid = atlasGuid;
        atlasObjectId = createAtlasObjectId();
    }

    public String getQualifiedName() {
        return toQualifiedName(rootProcessGroupId);
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void addConnection(ConnectionStatus c) {
        outGoingRelationShips.computeIfAbsent(c.getSourceId(), k -> new ArrayList<>()).add(c);
        incomingRelationShips.computeIfAbsent(c.getDestinationId(), k -> new ArrayList<>()).add(c);
    }

    public void addProcessor(ProcessorStatus p) {
        processors.put(p.getId(), p);
    }

    public Map<String, ProcessorStatus> getProcessors() {
        return processors;
    }

    public void addRemoteProcessGroup(RemoteProcessGroupStatus r) {
        remoteProcessGroups.put(r.getId(), r);
    }

    public void setFlowName(String flowName) {
        this.flowName = flowName;
    }

    public String getFlowName() {
        return flowName;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }

    public List<ConnectionStatus> getIncomingRelationShips(String componentId) {
        return incomingRelationShips.get(componentId);
    }

    public List<ConnectionStatus> getOutgoingRelationShips(String componentId) {
        return outGoingRelationShips.get(componentId);
    }

    public void addInputPort(PortStatus port) {
        inputPorts.put(port.getId(), port);
    }

    public Map<String, PortStatus> getInputPorts() {
        return inputPorts;
    }

    public void addOutputPort(PortStatus port) {
        outputPorts.put(port.getId(), port);
    }

    public Map<String, PortStatus> getOutputPorts() {
        return outputPorts;
    }

    public void addRootInputPort(PortStatus port) {
        rootInputPorts.put(port.getId(), port);
        createOrUpdateRootGroupPortEntity(true, toQualifiedName(port.getId()), port.getName());
    }

    public Map<String, PortStatus> getRootInputPorts() {
        return rootInputPorts;
    }

    public void addRootOutputPort(PortStatus port) {
        rootOutputPorts.put(port.getId(), port);
        createOrUpdateRootGroupPortEntity(false, toQualifiedName(port.getId()), port.getName());
    }

    public Map<String, PortStatus> getRootOutputPorts() {
        return rootOutputPorts;
    }

    public Map<AtlasObjectId, AtlasEntity> getRootInputPortEntities() {
        return rootInputPortEntities;
    }

    public Optional<AtlasObjectId> findIdByQualifiedName(Set<AtlasObjectId> ids, String qualifiedName) {
        return ids.stream().filter(id -> qualifiedName.equals(id.getUniqueAttributes().get(ATTR_QUALIFIED_NAME))).findFirst();
    }

    private AtlasEntity createOrUpdateRootGroupPortEntity(boolean isInput, String qualifiedName, String portName) {
        final Map<AtlasObjectId, AtlasEntity> ports = isInput ? rootInputPortEntities : rootOutputPortEntities;
        final Optional<AtlasObjectId> existingPortId = findIdByQualifiedName(ports.keySet(), qualifiedName);

        final String typeName = isInput ? TYPE_NIFI_INPUT_PORT : TYPE_NIFI_OUTPUT_PORT;

        if (existingPortId.isPresent()) {
            // TODO: Update port name and set updated flag.
            return ports.get(existingPortId.get());
        } else {
            final AtlasEntity entity = new AtlasEntity(typeName);

            entity.setAttribute(ATTR_NIFI_FLOW, getAtlasObjectId());
            entity.setAttribute(ATTR_NAME, portName);
            entity.setAttribute(ATTR_QUALIFIED_NAME, qualifiedName);

            final AtlasObjectId portId = new AtlasObjectId(typeName, ATTR_QUALIFIED_NAME, qualifiedName);
            ports.put(portId, entity);
            return entity;
        }
    }

    public Map<AtlasObjectId, AtlasEntity> getRootOutputPortEntities() {
        return rootOutputPortEntities;
    }

    public Tuple<AtlasObjectId, AtlasEntity> getOrCreateQueue(String destinationComponentId) {
        final String qualifiedName = toQualifiedName(destinationComponentId);
        final Optional<AtlasObjectId> existingQueueId = findIdByQualifiedName(queues.keySet(), qualifiedName);

        if (existingQueueId.isPresent()) {
            return new Tuple<>(existingQueueId.get(), queues.get(existingQueueId.get()));
        } else {
            final AtlasObjectId queueId = new AtlasObjectId(TYPE_NIFI_QUEUE, ATTR_QUALIFIED_NAME, qualifiedName);
            final AtlasEntity queue = new AtlasEntity(TYPE_NIFI_QUEUE);
            queue.setAttribute(ATTR_NIFI_FLOW, getAtlasObjectId());
            queue.setAttribute(ATTR_QUALIFIED_NAME, qualifiedName);
            queue.setAttribute(ATTR_NAME, "queue");
            queue.setAttribute(ATTR_DESCRIPTION, "Input queue for " + destinationComponentId);
            queues.put(queueId, queue);
            return new Tuple<>(queueId, queue);
        }
    }

    public Map<AtlasObjectId, AtlasEntity> getQueues() {
        return queues;
    }

    public Map<String, NiFiFlowPath> getFlowPaths() {
        return flowPaths;
    }

    /**
     * Find a flow_path that contains specified componentId.
     */
    public NiFiFlowPath findPath(String componentId) {
        for (NiFiFlowPath path: flowPaths.values()) {
            if (path.getProcessComponentIds().contains(componentId)){
                return path;
            }
        }
        return null;
    }

    /**
     * Determine if a component should be reported as NiFiFlowPath.
     */
    public boolean isProcessComponent(String componentId) {
        return isProcessor(componentId) || isRootInputPort(componentId) || isRootOutputPort(componentId);
    }

    public boolean isProcessor(String componentId) {
        return processors.containsKey(componentId);
    }

    public boolean isInputPort(String componentId) {
        return inputPorts.containsKey(componentId);
    }

    public boolean isOutputPort(String componentId) {
        return outputPorts.containsKey(componentId);
    }

    public boolean isRootInputPort(String componentId) {
        return rootInputPorts.containsKey(componentId);
    }

    public boolean isRootOutputPort(String componentId) {
        return rootOutputPorts.containsKey(componentId);
    }

    public String getProcessComponentName(String componentId) {
        return getProcessComponentName(componentId, () -> "unknown");
    }

    public String getProcessComponentName(String componentId, Supplier<String> unknown) {
        return isProcessor(componentId) ? getProcessors().get(componentId).getName()
                : isRootInputPort(componentId) ? getRootInputPorts().get(componentId).getName()
                : isRootOutputPort(componentId) ? getRootOutputPorts().get(componentId).getName() : unknown.get();
    }

    public String toQualifiedName(String componentId) {
        return componentId + "@" + clusterName;
    }

    public void dump() {
        logger.info("flowName: {}", flowName);
        Function<String, String> toName = pid -> processors.get(pid).getName();
        processors.forEach((pid, p) -> {
            logger.info("{}:{} receives from {}", pid, toName.apply(pid), incomingRelationShips.get(pid));
            logger.info("{}:{} sends to {}", pid, toName.apply(pid), outGoingRelationShips.get(pid));
        });
    }

}

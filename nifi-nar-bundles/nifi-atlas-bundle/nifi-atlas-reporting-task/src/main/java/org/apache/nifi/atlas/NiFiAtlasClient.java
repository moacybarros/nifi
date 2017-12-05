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

import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.nifi.atlas.security.AtlasAuthN;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MultivaluedMap;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_DESCRIPTION;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_FLOW_PATHS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_GUID;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_INPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_INPUT_PORTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUT_PORTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUEUES;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_TYPENAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_URL;
import static org.apache.nifi.atlas.NiFiTypes.ENTITIES;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW_PATH;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_INPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_OUTPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;

public class NiFiAtlasClient {

    private static final Logger logger = LoggerFactory.getLogger(NiFiAtlasClient.class);

    private static NiFiAtlasClient nifiClient;
    private AtlasClientV2 atlasClient;

    private NiFiAtlasClient() {
        super();
    }

    public static NiFiAtlasClient getInstance() {
        if (nifiClient == null) {
            synchronized (NiFiAtlasClient.class) {
                if (nifiClient == null) {
                    nifiClient = new NiFiAtlasClient();
                }
            }
        }
        return nifiClient;
    }

    public static class AtlasAuthNMethod {

    }

    public void initialize(final String[] baseUrls, final AtlasAuthN authN, final File atlasConfDir) {

        synchronized (NiFiAtlasClient.class) {

            if (atlasClient != null) {
                logger.info("{} had been setup but replacing it with new one.", atlasClient);
                ApplicationProperties.forceReload();
            }

            if (atlasConfDir != null) {
                // If atlasConfDir is not set, atlas-application.properties will be searched under classpath.
                Properties props = System.getProperties();
                final String atlasConfProp = "atlas.conf";
                props.setProperty(atlasConfProp, atlasConfDir.getAbsolutePath());
                logger.debug("{} has been set to: {}", atlasConfProp, props.getProperty(atlasConfProp));
            }

            atlasClient = authN.createClient(baseUrls);

        }
    }

    /**
     * This is an utility method to delete unused types.
     * Should be used during development or testing only.
     * @param typeNames to delete
     */
    void deleteTypeDefs(String ... typeNames) throws AtlasServiceException {
        final AtlasTypesDef existingTypeDef = getTypeDefs(typeNames);
        try {
            atlasClient.deleteAtlasTypeDefs(existingTypeDef);
        } catch (UniformInterfaceException e) {
            if (e.getResponse().getStatus() == 204) {
                // 204 is a successful response.
                // NOTE: However after executing this, Atlas should be restarted to work properly.
                logger.info("Deleted type defs: {}", existingTypeDef);
            } else {
                throw e;
            }
        }
    }

    /**
     * @return True when required NiFi types are already created.
     */
    public boolean isNiFiTypeDefsRegistered() throws AtlasServiceException {
        final Set<String> typeNames = ENTITIES.keySet();
        final Map<String, AtlasEntityDef> existingDefs = getTypeDefs(typeNames.toArray(new String[typeNames.size()])).getEntityDefs().stream()
                .collect(Collectors.toMap(def -> def.getName(), def -> def));
        return typeNames.stream().allMatch(typeName -> existingDefs.containsKey(typeName));
    }

    /**
     * Create or update NiFi types in Atlas type system.
     * @param update If false, doesn't perform anything if there is existing type def for the name.
     */
    public void registerNiFiTypeDefs(boolean update) throws AtlasServiceException {
        final Set<String> typeNames = ENTITIES.keySet();
        final Map<String, AtlasEntityDef> existingDefs = getTypeDefs(typeNames.toArray(new String[typeNames.size()])).getEntityDefs().stream()
                .collect(Collectors.toMap(def -> def.getName(), def -> def));


        final AtomicBoolean shouldUpdate = new AtomicBoolean(false);

        final AtlasTypesDef type = new AtlasTypesDef();

        typeNames.stream().filter(typeName -> {
            final AtlasEntityDef existingDef = existingDefs.get(typeName);
            if (existingDef != null) {
                // type is already defined.
                if (!update) {
                    return false;
                }
                shouldUpdate.set(true);
            }
            return true;
        }).forEach(typeName -> {
            final NiFiTypes.EntityDefinition def = ENTITIES.get(typeName);

            final AtlasEntityDef entity = new AtlasEntityDef();
            type.getEntityDefs().add(entity);

            entity.setName(typeName);

            Set<String> superTypes = new HashSet<>();
            List<AtlasAttributeDef> attributes = new ArrayList<>();

            def.define(entity, superTypes, attributes);

            entity.setSuperTypes(superTypes);
            entity.setAttributeDefs(attributes);
        });

        // Create or Update.
        final AtlasTypesDef atlasTypeDefsResult = shouldUpdate.get()
                ? atlasClient.updateAtlasTypeDefs(type)
                : atlasClient.createAtlasTypeDefs(type);
        logger.debug("Result={}", atlasTypeDefsResult);
    }

    private AtlasTypesDef getTypeDefs(String ... typeNames) throws AtlasServiceException {
        final AtlasTypesDef typeDefs = new AtlasTypesDef();
        for (int i = 0; i < typeNames.length; i++) {
            final MultivaluedMap<String, String> searchParams = new MultivaluedMapImpl();
            searchParams.add(SearchFilter.PARAM_NAME, typeNames[i]);
            final AtlasTypesDef typeDef = atlasClient.getAllTypeDefs(new SearchFilter(searchParams));
            typeDefs.getEntityDefs().addAll(typeDef.getEntityDefs());
        }
        logger.debug("typeDefs={}", typeDefs);
        return typeDefs;
    }

    private String toStr(Object obj) {
        return obj != null ? obj.toString() : null;
    }

    public NiFiFlow fetchNiFiFlow(String rootProcessGroupId, String clusterName) throws AtlasServiceException {
        final String qualifiedName = rootProcessGroupId + "@" + clusterName;
        final AtlasObjectId flowId = new AtlasObjectId(TYPE_NIFI_FLOW, ATTR_QUALIFIED_NAME, qualifiedName);
        final AtlasEntity.AtlasEntityWithExtInfo nifiFlowExt = searchEntityDef(flowId);

        if (nifiFlowExt == null || nifiFlowExt.getEntity() == null) {
            return null;
        }

        final AtlasEntity nifiFlowEntity = nifiFlowExt.getEntity();
        final Map<String, Object> attributes = nifiFlowEntity.getAttributes();
        final NiFiFlow nifiFlow = new NiFiFlow(rootProcessGroupId);
        nifiFlow.setExEntity(nifiFlowEntity);
        nifiFlow.setFlowName(toStr(attributes.get(ATTR_NAME)));
        nifiFlow.setClusterName(clusterName);
        nifiFlow.setUrl(toStr(attributes.get(ATTR_URL)));

        nifiFlow.getQueues().putAll(toQualifiedNameIds(toAtlasObjectIds(nifiFlowEntity.getAttribute(ATTR_QUEUES))));
        nifiFlow.getRootInputPortEntities().putAll(toQualifiedNameIds(toAtlasObjectIds(nifiFlowEntity.getAttribute(ATTR_INPUT_PORTS))));
        nifiFlow.getRootOutputPortEntities().putAll(toQualifiedNameIds(toAtlasObjectIds(nifiFlowEntity.getAttribute(ATTR_OUTPUTS))));

        final Map<String, NiFiFlowPath> flowPaths = nifiFlow.getFlowPaths();
        final List<AtlasObjectId> flowPathIds = toAtlasObjectIds(attributes.get(ATTR_FLOW_PATHS));

        for (AtlasObjectId flowPathId : flowPathIds) {
            final AtlasEntity.AtlasEntityWithExtInfo flowPathExt = searchEntityDef(flowPathId);
            if (flowPathExt == null || flowPathExt.getEntity() == null) {
                continue;
            }
            final AtlasEntity flowPathEntity = flowPathExt.getEntity();
            final String[] pathQnameSplit = toStr(flowPathEntity.getAttribute(ATTR_QUALIFIED_NAME)).split("@");
            final NiFiFlowPath flowPath = new NiFiFlowPath(pathQnameSplit[0]);
            flowPath.setExEntity(flowPathEntity);
            flowPath.setName(toStr(flowPathEntity.getAttribute(ATTR_NAME)));
            flowPath.getInputs().addAll(toQualifiedNameIds(toAtlasObjectIds(flowPathEntity.getAttribute(ATTR_INPUTS))).keySet());
            flowPath.getOutputs().addAll(toQualifiedNameIds(toAtlasObjectIds(flowPathEntity.getAttribute(ATTR_OUTPUTS))).keySet());
            flowPaths.put(flowPath.getId(), flowPath);
        }
        return nifiFlow;
    }

    @SuppressWarnings("unchecked")
    private List<AtlasObjectId> toAtlasObjectIds(Object _references) {
        if (_references == null) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> references = (List<Map<String, Object>>) _references;
        return references.stream()
                .map(ref -> new AtlasObjectId(toStr(ref.get(ATTR_GUID)), toStr(ref.get(ATTR_TYPENAME)), ref))
                .collect(Collectors.toList());
    }

    /**
     * AtlasObjectIds returned from Atlas have GUID, but do not have qualifiedName, while ones created by the reporting task
     * do not have GUID, but qualifiedName. AtlasObjectId.equals returns false for this combination.
     * In order to match ids correctly, this method converts input ids into ones with qualifiedName attribute.
     * @param ids to convert
     * @return AtlasObjectIds with qualifiedName
     */
    private Map<AtlasObjectId, AtlasEntity> toQualifiedNameIds(List<AtlasObjectId> ids) {
        if (ids == null) {
            return Collections.emptyMap();
        }

        return ids.stream().distinct().map(id -> {
            try {
                final AtlasEntity.AtlasEntityWithExtInfo entityExt = searchEntityDef(id);
                final AtlasEntity entity = entityExt.getEntity();
                final Map<String, Object> uniqueAttrs = Collections.singletonMap(ATTR_QUALIFIED_NAME, entity.getAttribute(ATTR_QUALIFIED_NAME));
                return new Tuple<>(new AtlasObjectId(id.getGuid(), id.getTypeName(), uniqueAttrs), entity);
            } catch (AtlasServiceException e) {
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toMap(Tuple::getKey, Tuple::getValue));
    }

    public void registerNiFiFlow(NiFiFlow nifiFlow) throws AtlasServiceException {

        // Create parent flow entity, so that common properties are taken over.
        final AtlasEntity flowEntity = registerNiFiFlowEntity(nifiFlow);

        // Create DataSet entities those are created by this NiFi flow.
        registerDataSetEntities(nifiFlow);

        // Create path entities.
        registerFlowPathEntities(nifiFlow);

        // Now loop through entities again to add relationships.
        // TODO: If anything has been added in this cycle.
        final Set<AtlasObjectId> pathIds = nifiFlow.getFlowPaths().values().stream()
                .map(path -> new AtlasObjectId(path.getAtlasGuid(), TYPE_NIFI_FLOW_PATH,
                        Collections.singletonMap(ATTR_QUALIFIED_NAME, nifiFlow.toQualifiedName(path.getId()))))
                .collect(Collectors.toSet());
        flowEntity.setAttribute(ATTR_FLOW_PATHS, pathIds);
        flowEntity.setAttribute(ATTR_QUEUES, nifiFlow.getQueues().keySet());
        flowEntity.setAttribute(ATTR_INPUT_PORTS, nifiFlow.getRootInputPortEntities().keySet());
        flowEntity.setAttribute(ATTR_OUTPUT_PORTS, nifiFlow.getRootOutputPortEntities().keySet());

        // Send updated entities.
        final List<AtlasEntity> entities = new ArrayList<>();
        final AtlasEntity.AtlasEntitiesWithExtInfo atlasEntities = new AtlasEntity.AtlasEntitiesWithExtInfo(entities);
        entities.add(flowEntity);
        try {
            final EntityMutationResponse mutationResponse = atlasClient.createEntities(atlasEntities);
            logger.debug("mutation response={}", mutationResponse);
        } catch (AtlasServiceException e) {
            if (e.getStatus().getStatusCode() == 404 && e.getMessage().contains("ATLAS-404-00-00B")) {
                // NOTE: If previously existed nifi_flow_path entity is removed because the path is removed from NiFi,
                // then Atlas respond with 404 even though the entity is successfully updated.
                // Following exception is thrown in this case. Just log it.
                // org.apache.atlas.AtlasServiceException:
                // Metadata service API org.apache.atlas.AtlasBaseClient$APIInfo@45a37759
                // failed with status 404 (Not Found) Response Body
                // ({"errorCode":"ATLAS-404-00-00B","errorMessage":"Given instance is invalid/not found:
                // Could not find entities in the repository with guids: [96d24487-cd66-4795-b552-f00b426fed26]"})
                logger.debug("Received error response from Atlas but it should be stored." + e);
            } else {
                throw e;
            }
        }
    }

    private AtlasEntity registerNiFiFlowEntity(final NiFiFlow nifiFlow) throws AtlasServiceException {
        final List<AtlasEntity> entities = new ArrayList<>();
        final AtlasEntity.AtlasEntitiesWithExtInfo atlasEntities = new AtlasEntity.AtlasEntitiesWithExtInfo(entities);

        // Create parent flow entity using existing NiFiFlow entity if available, so that common properties are taken over.
        final AtlasEntity flowEntity = nifiFlow.getExEntity() != null ? new AtlasEntity(nifiFlow.getExEntity()) : new AtlasEntity();
        flowEntity.setTypeName(TYPE_NIFI_FLOW);
        flowEntity.setVersion(1L);
        flowEntity.setAttribute(ATTR_NAME, nifiFlow.getFlowName());
        flowEntity.setAttribute(ATTR_QUALIFIED_NAME, nifiFlow.toQualifiedName(nifiFlow.getRootProcessGroupId()));
        flowEntity.setAttribute(ATTR_URL, nifiFlow.getUrl());
        flowEntity.setAttribute(ATTR_DESCRIPTION, nifiFlow.getDescription());

        // If flowEntity is not persisted yet, then store nifi_flow entity to make nifiFlowId available for other entities.
        if (flowEntity.getGuid().startsWith("-")) {
            entities.add(flowEntity);
            final EntityMutationResponse mutationResponse = atlasClient.createEntities(atlasEntities);
            logger.debug("Registered a new nifi_flow entity, mutation response={}", mutationResponse);
            final String assignedNiFiFlowGuid = mutationResponse.getGuidAssignments().get(flowEntity.getGuid());
            flowEntity.setGuid(assignedNiFiFlowGuid);
            nifiFlow.setAtlasGuid(assignedNiFiFlowGuid);
        }

        return flowEntity;
    }

    private void registerDataSetEntities(final NiFiFlow nifiFlow) throws AtlasServiceException {
        final Map<AtlasObjectId, AtlasEntity> inputPorts = nifiFlow.getRootInputPortEntities();
        final Map<AtlasObjectId, AtlasEntity> outputPorts = nifiFlow.getRootOutputPortEntities();
        final Map<AtlasObjectId, AtlasEntity> queues = nifiFlow.getQueues();

        final List<AtlasEntity> entities = Stream.of(inputPorts.values().stream(), outputPorts.values().stream(), queues.values().stream()).flatMap(Function.identity())
                .filter(entity -> entity.getGuid().startsWith("-")).collect(Collectors.toList());
        final AtlasEntity.AtlasEntitiesWithExtInfo atlasEntities = new AtlasEntity.AtlasEntitiesWithExtInfo(entities);

        final EntityMutationResponse mutationResponse = atlasClient.createEntities(atlasEntities);
        logger.debug("mutation response={}", mutationResponse);

        final Map<String, String> guidAssignments = mutationResponse.getGuidAssignments();
        for (AtlasEntity entity : entities) {
            final Map<AtlasObjectId, AtlasEntity> entityMap;
            switch (entity.getTypeName()) {
                case TYPE_NIFI_INPUT_PORT:
                    entityMap = nifiFlow.getRootInputPortEntities();
                    break;
                case TYPE_NIFI_OUTPUT_PORT:
                    entityMap = nifiFlow.getRootOutputPortEntities();
                    break;
                case TYPE_NIFI_QUEUE:
                    entityMap = nifiFlow.getQueues();
                    break;
                default:
                    throw new RuntimeException(entity.getTypeName() + " is not expected.");
            }
            final String qualifiedName = toStr(entity.getAttribute(ATTR_QUALIFIED_NAME));
            final Optional<AtlasObjectId> originalId = nifiFlow.findIdByQualifiedName(entityMap.keySet(), qualifiedName);
            if (originalId.isPresent()) {
                entityMap.remove(originalId.get());
            }
            final String guid = guidAssignments.get(entity.getGuid());
            entity.setGuid(guid);
            final AtlasObjectId idWithGuid = new AtlasObjectId(guid, entity.getTypeName(), Collections.singletonMap(ATTR_QUALIFIED_NAME, qualifiedName));
            entityMap.put(idWithGuid, entity);

        }
    }

    private void registerFlowPathEntities(final NiFiFlow nifiFlow) throws AtlasServiceException {
        final List<AtlasEntity> entities = new ArrayList<>();
        final AtlasEntity.AtlasEntitiesWithExtInfo atlasEntities = new AtlasEntity.AtlasEntitiesWithExtInfo(entities);
        for (NiFiFlowPath path : nifiFlow.getFlowPaths().values()) {
            // Create path entities with existing path.
            final AtlasEntity pathEntity = path.getExEntity() != null ? new AtlasEntity(path.getExEntity()) : new AtlasEntity();
            entities.add(pathEntity);
            pathEntity.setTypeName(TYPE_NIFI_FLOW_PATH);
            pathEntity.setVersion(1L);
            pathEntity.setAttribute(ATTR_NIFI_FLOW, nifiFlow.getAtlasObjectId());

            final StringBuilder name = new StringBuilder();
            final StringBuilder description = new StringBuilder();
            path.getProcessComponentIds().forEach(pid -> {
                final String componentName = nifiFlow.getProcessComponentName(pid);

                if (name.length() > 0) {
                    name.append(", ");
                    description.append(", ");
                }
                name.append(componentName);
                description.append(String.format("%s::%s", componentName, pid));
            });

            path.setName(name.toString());
            pathEntity.setAttribute(ATTR_NAME, name.toString());
            pathEntity.setAttribute(ATTR_DESCRIPTION, description.toString());

            // Use first processor's id as qualifiedName.
            pathEntity.setAttribute(ATTR_QUALIFIED_NAME, nifiFlow.toQualifiedName(path.getId()));

            pathEntity.setAttribute(ATTR_URL, path.createDeepLinkURL(nifiFlow.getUrl()));

            pathEntity.setAttribute(ATTR_INPUTS, path.getInputs());
            pathEntity.setAttribute(ATTR_OUTPUTS, path.getOutputs());
        }

        // Create entities without relationships, Atlas doesn't allow storing ObjectId that doesn't exist.
        final EntityMutationResponse mutationResponse = atlasClient.createEntities(atlasEntities);
        logger.debug("mutation response={}", mutationResponse);
        // TODO: get assigned guid.
    }

    public AtlasEntity.AtlasEntityWithExtInfo searchEntityDef(AtlasObjectId id) throws AtlasServiceException {
        final String guid = id.getGuid();
        if (!StringUtils.isEmpty(guid)) {
            return atlasClient.getEntityByGuid(guid);
        }
        final Map<String, String> attributes = new HashMap<>();
        id.getUniqueAttributes().entrySet().stream().filter(entry -> entry.getValue() != null)
                .forEach(entry -> attributes.put(entry.getKey(), entry.getValue().toString()));
        return atlasClient.getEntityByAttribute(id.getTypeName(), attributes);
    }

}

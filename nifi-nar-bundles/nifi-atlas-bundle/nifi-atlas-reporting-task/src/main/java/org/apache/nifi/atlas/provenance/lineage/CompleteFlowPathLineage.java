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

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.NiFiFlow;
import org.apache.nifi.atlas.NiFiFlowPath;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.lineage.ComputeLineageResult;
import org.apache.nifi.provenance.lineage.LineageNode;
import org.apache.nifi.provenance.lineage.LineageNodeType;
import org.apache.nifi.util.Tuple;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;
import static org.apache.nifi.provenance.ProvenanceEventType.DROP;

public class CompleteFlowPathLineage extends AbstractLineageStrategy {

    @Override
    public ProvenanceEventType[] getTargetEventTypes() {
        return new ProvenanceEventType[]{DROP};
    }

    @Override
    public void processEvent(AnalysisContext analysisContext, NiFiFlow nifiFlow, ProvenanceEventRecord event) {
        if (!ProvenanceEventType.DROP.equals(event.getEventType())) {
            return;
        }
        final ComputeLineageResult lineage = analysisContext.queryLineage(event.getEventId());

        // Construct a tree model to traverse backwards.
        final Map<String, List<LineageNode>> lineageTree = new HashMap<>();
        analyzeLineageTree(lineage, lineageTree);

        final LineagePath lineagePath = new LineagePath();
        extractLineagePaths(analysisContext, lineageTree, lineagePath, event);

        analyzeLineagePath(analysisContext, lineagePath);

        // Input and output data set are both required to report lineage.
        List<Tuple<NiFiFlowPath, DataSetRefs>> createdFlowPaths = new ArrayList<>();
        if (lineagePath.isComplete()) {
            addDataSetRefs(nifiFlow, lineagePath, createdFlowPaths);
            for (Tuple<NiFiFlowPath, DataSetRefs> createdFlowPath : createdFlowPaths) {
                final NiFiFlowPath flowPath = createdFlowPath.getKey();
                createEntity(toReferenceable(flowPath, nifiFlow));
                addDataSetRefs(nifiFlow, Collections.singleton(flowPath), createdFlowPath.getValue());
            }
            createdFlowPaths.clear();
        }
    }

    private List<LineageNode> findParentEvents(Map<String, List<LineageNode>> lineageTree, ProvenanceEventRecord event) {
        List<LineageNode> parentNodes = lineageTree.get(String.valueOf(event.getEventId()));
        List<LineageNode> parentEvents = parentNodes == null || parentNodes.isEmpty() ? null : parentNodes.stream()
                // In case it's not a provenance event (i.e. FLOWFILE_NODE), get one level higher parents.
                .flatMap(n -> !LineageNodeType.PROVENANCE_EVENT_NODE.equals(n.getNodeType())
                        ? lineageTree.get(n.getIdentifier()).stream() : Stream.of(n))
                .collect(Collectors.toList());
        return parentEvents;
    }

    private void extractLineagePaths(AnalysisContext context, Map<String, List<LineageNode>> lineageTree,
                                     LineagePath lineagePath, ProvenanceEventRecord lastEvent) {

        lineagePath.getEvents().add(lastEvent);
        List<LineageNode> parentEvents = findParentEvents(lineageTree, lastEvent);

        final boolean createSeparateParentPath;
        if (parentEvents == null || parentEvents.isEmpty()) {

            switch (lastEvent.getEventType()) {
                case JOIN:
                case FORK:
                    // Try expanding the lineage.
                    final ComputeLineageResult joinedParents = context.findParents(lastEvent.getEventId());
                    analyzeLineageTree(joinedParents, lineageTree);

                    parentEvents = findParentEvents(lineageTree, lastEvent);

                    if (parentEvents == null || parentEvents.isEmpty()) {
                        logger.debug("Expanded parents from {} but couldn't find any.", lastEvent);
                        return;
                    }

                    // With these events, always create separate parents regardless of the number of parents.
                    createSeparateParentPath = true;
                    break;

                default:
                    return;
            }

        } else {
            createSeparateParentPath = parentEvents.size() > 1;
        }

        if (createSeparateParentPath) {
            // Treat those as separated lineage_path
            parentEvents.stream()
                    .map(parentEvent -> context.getProvenanceEvent(Long.parseLong(parentEvent.getIdentifier())))
                    .filter(Objects::nonNull)
                    .forEach(parent -> {
                        final LineagePath parentPath = new LineagePath();
                        lineagePath.getParents().add(parentPath);
                        extractLineagePaths(context, lineageTree, parentPath, parent);
                    });
        } else {
            // Simply traverse upwards.
            final ProvenanceEventRecord parentEvent = context.getProvenanceEvent(Long.parseLong(parentEvents.get(0).getIdentifier()));
            if (parentEvent != null) {
                extractLineagePaths(context, lineageTree, lineagePath, parentEvent);
            }
        }
    }

    private void analyzeLineagePath(AnalysisContext analysisContext, LineagePath lineagePath) {
        final List<ProvenanceEventRecord> events = lineagePath.getEvents();

        final DataSetRefs parentRefs = new DataSetRefs(events.get(0).getComponentId());
        events.forEach(event -> {
            final DataSetRefs refs = executeAnalyzer(analysisContext, event);
            if (refs == null || refs.isEmpty()) {
                return;
            }
            refs.getInputs().forEach(i -> parentRefs.addInput(i));
            refs.getOutputs().forEach(o -> parentRefs.addOutput(o));
        });

        lineagePath.setRefs(parentRefs);

        // Analyse parents.
        lineagePath.getParents().forEach(parent -> analyzeLineagePath(analysisContext, parent));
    }

    private void analyzeLineageTree(ComputeLineageResult lineage, Map<String, List<LineageNode>> lineageTree) {
        lineage.getEdges().forEach(edge -> lineageTree
                        .computeIfAbsent(edge.getDestination().getIdentifier(), k -> new ArrayList<>())
                        .add(edge.getSource()));
    }

    private void addDataSetRefs(NiFiFlow nifiFlow, LineagePath lineagePath, List<Tuple<NiFiFlowPath, DataSetRefs>> createdFlowPaths) {

        final List<ProvenanceEventRecord> events = lineagePath.getEvents();
        Collections.reverse(events);

        final List<String> componentIds = events.stream().map(event -> event.getComponentId()).collect(Collectors.toList());
        final String firstComponentId = componentIds.get(0);

        // Process parents first for two reasons.
        // First, this lineagePath needs parent reference.
        // Second, as parents are significant to distinguish a path from another.
        // For example, even if two lineagePaths have identical componentIds/inputs/outputs,
        // if those parents have different inputs, those should be treated as different paths.
        Referenceable queueBetweenParent = null;
        if (!lineagePath.getParents().isEmpty()) {
            // Add queue between this lineage path and parent.
            queueBetweenParent = new Referenceable(TYPE_NIFI_QUEUE);
            // The first event knows why this lineage has parents, e.g. FORK or JOIN.
            final String firstEventType = events.get(0).getEventType().name();
            queueBetweenParent.set(ATTR_NAME, firstEventType);
            lineagePath.getRefs().addInput(queueBetweenParent);

            for (LineagePath parent : lineagePath.getParents()) {
                parent.getRefs().addOutput(queueBetweenParent);
                addDataSetRefs(nifiFlow, parent, createdFlowPaths);
            }
        }

        // Create a variant path.
        // Calculate a hash from component_ids and input and output resource ids.
        final Stream<String> ioIds = Stream.concat(lineagePath.getRefs().getInputs().stream(), lineagePath.getRefs().getOutputs()
                .stream()).map(ref -> ref.getTypeName() + "::" + ref.get(ATTR_QUALIFIED_NAME));

        final Stream<String> parentHashes = lineagePath.getParents().stream().map(p -> String.valueOf(p.getLineagePathHash()));
        final CRC32 crc32 = new CRC32();
        // TODO: add parent hash to hash
        crc32.update(Stream.of(componentIds.stream(), ioIds, parentHashes).reduce(Stream::concat).orElseGet(Stream::empty)
                .sorted().distinct()
                .collect(Collectors.joining(",")).getBytes(StandardCharsets.UTF_8));

        final long hash = crc32.getValue();
        lineagePath.setLineagePathHash(hash);
        final NiFiFlowPath flowPath = new NiFiFlowPath(firstComponentId, hash);

        // In order to differentiate a queue between parents and this flow_path, add the hash into the queue qname.
        // E.g, FF1 and FF2 read from dirA were merged, vs FF3 and FF4 read from dirB were merged then passed here, these two should be different queue.
        if (queueBetweenParent != null) {
            queueBetweenParent.set(ATTR_QUALIFIED_NAME, firstComponentId + "::" + hash);
        }

        // Different componentIds can identify the same flow_path, so make it unique here.
        final Set<String> uniquePathIds = new HashSet<>();
        final String pathName = componentIds.stream()
                .map(componentId -> nifiFlow.findPath(componentId))
                .filter(Objects::nonNull)
                // Skip a path if it's already added to the name.
                .filter(path -> !uniquePathIds.contains(path.getId()))
                .map(path -> {
                    uniquePathIds.add(path.getId());
                    return path.getName();
                })
                .collect(Collectors.joining(" -> "));

        flowPath.setName(pathName);
        flowPath.setGroupId(nifiFlow.findPath(firstComponentId).getGroupId());

        // To defer send notification until entire lineagePath analysis gets finished, just add the instance into a buffer.
        createdFlowPaths.add(new Tuple<>(flowPath, lineagePath.getRefs()));
    }
}

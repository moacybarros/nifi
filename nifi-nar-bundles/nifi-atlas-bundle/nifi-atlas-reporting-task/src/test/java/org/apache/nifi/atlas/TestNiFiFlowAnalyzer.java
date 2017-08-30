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

import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.flow.FlowBreadcrumbDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.entity.ComponentEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.FlowBreadcrumbEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class TestNiFiFlowAnalyzer {

    private int componentId = 0;
    private AtlasVariables atlasVariables;

    @Before
    public void before() throws Exception {
        componentId = 0;
        atlasVariables = new AtlasVariables();
    }

    private ProcessGroupFlowEntity createEmptyProcessGroupFlowEntity() {
        ProcessGroupFlowEntity pgEntity = new ProcessGroupFlowEntity();
        final PermissionsDTO permissions = new PermissionsDTO();
        permissions.setCanRead(true);
        pgEntity.setPermissions(permissions);

        ProcessGroupFlowDTO pgFlow = new ProcessGroupFlowDTO();
        pgFlow.setId(nextComponentId());
        FlowBreadcrumbEntity breadcrumb = new FlowBreadcrumbEntity();
        FlowBreadcrumbDTO breadcrumbDTO = new FlowBreadcrumbDTO();
        breadcrumbDTO.setName("Flow name");
        breadcrumb.setBreadcrumb(breadcrumbDTO);
        pgFlow.setBreadcrumb(breadcrumb);
        pgEntity.setProcessGroupFlow(pgFlow);

        FlowDTO flow = new FlowDTO();
        pgFlow.setFlow(flow);
        return pgEntity;
    }

    private ProcessGroupEntity createProcessGroupEntity() {
        final ProcessGroupEntity entity = new ProcessGroupEntity();
        ProcessGroupDTO processGroup = new ProcessGroupDTO();
        processGroup.setComments("Flow comment");
        entity.setComponent(processGroup);
        return entity;
    }

    @Test
    public void testEmptyFlow() throws Exception {
        NiFiApiClient nifiApiClient = Mockito.mock(NiFiApiClient.class);

        ProcessGroupFlowEntity rootPGEntity = createEmptyProcessGroupFlowEntity();

        when(nifiApiClient.getProcessGroupFlow()).thenReturn(rootPGEntity);
        when(nifiApiClient.getProcessGroupEntity()).thenReturn(createProcessGroupEntity());

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer(nifiApiClient);

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(atlasVariables);

        assertEquals("Flow name", nifiFlow.getFlowName());
        assertEquals("Flow comment", nifiFlow.getDescription());
    }

    private ProcessorEntity createProcessor(ProcessGroupFlowEntity pgEntity, String type) {
        final ProcessorEntity processor = new ProcessorEntity();
        pgEntity.getProcessGroupFlow().getFlow().getProcessors().add(processor);
        processor.setId(nextComponentId());

        ProcessorDTO processorDTO =  new ProcessorDTO();
        processor.setComponent(processorDTO);
        processorDTO.setId(processor.getId());
        processorDTO.setType(type);
        ProcessorConfigDTO config = new ProcessorConfigDTO();
        processorDTO.setConfig(config);
        return  processor;
    }

    private String nextComponentId() {
        return String.format("1234-5678-0000-%04d", componentId++);
    }

    private String getConnectionType(ComponentEntity o) {
        if (o instanceof ProcessorEntity) {
            return "PROCESSOR";
        } else if (o instanceof PortEntity) {
            return ((PortEntity) o).getPortType();
        } else {
            throw new IllegalArgumentException("Not supported.");
        }
    }

    private void connect(ProcessGroupFlowEntity rootPGEntity, ComponentEntity pr0, ComponentEntity pr1) {
        connect(rootPGEntity, pr0, rootPGEntity, pr1);
    }

    private void connect(ProcessGroupFlowEntity pg0, ComponentEntity pr0, ProcessGroupFlowEntity pg1, ComponentEntity pr1) {
        ConnectionEntity conn = new ConnectionEntity();
        conn.setId(nextComponentId());
        pg0.getProcessGroupFlow().getFlow().getConnections().add(conn);
        pg1.getProcessGroupFlow().getFlow().getConnections().add(conn);

        ConnectionDTO connDTO = new ConnectionDTO();
        conn.setComponent(connDTO);
        connDTO.setId(conn.getId());

        ConnectableDTO source = new ConnectableDTO();
        source.setId(pr0.getId());
        source.setType(getConnectionType(pr0));
        source.setGroupId(pg0.getProcessGroupFlow().getId());
        connDTO.setSource(source);

        ConnectableDTO dest = new ConnectableDTO();
        dest.setId(pr1.getId());
        dest.setType(getConnectionType(pr1));
        dest.setGroupId(pg1.getProcessGroupFlow().getId());
        connDTO.setDestination(dest);
    }

    @Test
    public void testSingleProcessor() throws Exception {

        NiFiApiClient nifiApiClient = Mockito.mock(NiFiApiClient.class);

        ProcessGroupFlowEntity rootPGEntity = createEmptyProcessGroupFlowEntity();

        final ProcessorEntity pr0 = createProcessor(rootPGEntity, "org.apache.nifi.processors.standard.GenerateFlowFile");

        when(nifiApiClient.getProcessGroupFlow()).thenReturn(rootPGEntity);

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer(nifiApiClient);

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(atlasVariables);

        assertEquals(1, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final List<NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        assertEquals(1, paths.size());

        final NiFiFlowPath path0 = paths.get(0);
        assertEquals("p0", path0.getName());
        assertEquals(path0.getId(), path0.getProcessorIds().get(0));

        // Should be able to find a path from a given processor GUID.
        final NiFiFlowPath pathForPr0 = nifiFlow.findPath(pr0.getId());
        assertEquals(path0, pathForPr0);
    }


    @Test
    public void testProcessorsWithinSinglePath() throws Exception {

        NiFiApiClient nifiApiClient = Mockito.mock(NiFiApiClient.class);

        ProcessGroupFlowEntity rootPGEntity = createEmptyProcessGroupFlowEntity();

        final ProcessorEntity pr0 = createProcessor(rootPGEntity, "org.apache.nifi.processors.standard.GenerateFlowFile");
        final ProcessorEntity pr1 = createProcessor(rootPGEntity, "org.apache.nifi.processors.standard.UpdateAttribute");

        connect(rootPGEntity, pr0, pr1);

        when(nifiApiClient.getProcessGroupFlow()).thenReturn(rootPGEntity);

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer(nifiApiClient);

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(atlasVariables);

        assertEquals(2, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final List<NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        assertEquals(1, paths.size());

        // Should be able to find a path from a given processor GUID.
        final NiFiFlowPath pathForPr0 = nifiFlow.findPath(pr0.getId());
        final NiFiFlowPath pathForPr1 = nifiFlow.findPath(pr1.getId());
        final NiFiFlowPath path0 = paths.get(0);
        assertEquals(path0, pathForPr0);
        assertEquals(path0, pathForPr1);
    }

    @Test
    public void testMultiPaths() throws Exception {

        NiFiApiClient nifiApiClient = Mockito.mock(NiFiApiClient.class);

        ProcessGroupFlowEntity rootPGEntity = createEmptyProcessGroupFlowEntity();

        final ProcessorEntity pr0 = createProcessor(rootPGEntity, "org.apache.nifi.processors.standard.GenerateFlowFile");
        final ProcessorEntity pr1 = createProcessor(rootPGEntity, "org.apache.nifi.processors.standard.UpdateAttribute");
        final ProcessorEntity pr2 = createProcessor(rootPGEntity, "org.apache.nifi.processors.standard.ListenTCP");
        final ProcessorEntity pr3 = createProcessor(rootPGEntity, "org.apache.nifi.processors.standard.LogAttribute");

        connect(rootPGEntity, pr0, pr1);
        connect(rootPGEntity, pr2, pr3);

        when(nifiApiClient.getProcessGroupFlow()).thenReturn(rootPGEntity);

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer(nifiApiClient);

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(atlasVariables);

        assertEquals(4, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final List<NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        assertEquals(2, paths.size());

        // Order is not guaranteed
        final Map<String, NiFiFlowPath> pathMap = paths.stream().collect(Collectors.toMap(p -> p.getId(), p -> p));
        final NiFiFlowPath pathA = pathMap.get(pr0.getId());
        final NiFiFlowPath pathB = pathMap.get(pr2.getId());
        assertEquals(2, pathA.getProcessorIds().size());
        assertEquals(2, pathB.getProcessorIds().size());

        // Should be able to find a path from a given processor GUID.
        final NiFiFlowPath pathForPr0 = nifiFlow.findPath(pr0.getId());
        final NiFiFlowPath pathForPr1 = nifiFlow.findPath(pr1.getId());
        final NiFiFlowPath pathForPr2 = nifiFlow.findPath(pr2.getId());
        final NiFiFlowPath pathForPr3 = nifiFlow.findPath(pr3.getId());
        assertEquals(pathA, pathForPr0);
        assertEquals(pathA, pathForPr1);
        assertEquals(pathB, pathForPr2);
        assertEquals(pathB, pathForPr3);
    }

    @Test
    public void testMultiPathsJoint() throws Exception {

        NiFiApiClient nifiApiClient = Mockito.mock(NiFiApiClient.class);

        ProcessGroupFlowEntity rootPGEntity = createEmptyProcessGroupFlowEntity();

        final ProcessorEntity pr0 = createProcessor(rootPGEntity, "org.apache.nifi.processors.standard.GenerateFlowFile");
        final ProcessorEntity pr1 = createProcessor(rootPGEntity, "org.apache.nifi.processors.standard.UpdateAttribute");
        final ProcessorEntity pr2 = createProcessor(rootPGEntity, "org.apache.nifi.processors.standard.ListenTCP");
        final ProcessorEntity pr3 = createProcessor(rootPGEntity, "org.apache.nifi.processors.standard.LogAttribute");

        // Result should be as follows:
        // pathA = 0 -> 1 (-> 3)
        // pathB = 2 (-> 3)
        // pathC = 3
        connect(rootPGEntity, pr0, pr1);
        connect(rootPGEntity, pr1, pr3);
        connect(rootPGEntity, pr2, pr3);

        when(nifiApiClient.getProcessGroupFlow()).thenReturn(rootPGEntity);

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer(nifiApiClient);

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(atlasVariables);

        assertEquals(4, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final List<NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        assertEquals(3, paths.size());

        // Order is not guaranteed
        final Map<String, NiFiFlowPath> pathMap = paths.stream().collect(Collectors.toMap(p -> p.getId(), p -> p));
        final NiFiFlowPath pathA = pathMap.get(pr0.getId());
        final NiFiFlowPath pathB = pathMap.get(pr2.getId());
        final NiFiFlowPath pathC = pathMap.get(pr3.getId());
        assertEquals(2, pathA.getProcessorIds().size());
        assertEquals(1, pathB.getProcessorIds().size());
        assertEquals(1, pathC.getProcessorIds().size());

        // A queue is added as input for the joint point.
        assertEquals(1, pathC.getInputs().size());
        final AtlasObjectId queue = pathC.getInputs().iterator().next();
        assertEquals(TYPE_NIFI_QUEUE, queue.getTypeName());
        assertEquals(pathC.getId(), queue.getUniqueAttributes().get(ATTR_QUALIFIED_NAME));

        // Should be able to find a path from a given processor GUID.
        final NiFiFlowPath pathForPr0 = nifiFlow.findPath(pr0.getId());
        final NiFiFlowPath pathForPr1 = nifiFlow.findPath(pr1.getId());
        final NiFiFlowPath pathForPr2 = nifiFlow.findPath(pr2.getId());
        final NiFiFlowPath pathForPr3 = nifiFlow.findPath(pr3.getId());
        assertEquals(pathA, pathForPr0);
        assertEquals(pathA, pathForPr1);
        assertEquals(pathB, pathForPr2);
        assertEquals(pathC, pathForPr3);
    }

    @Test
    public void testRootGroupPorts() throws Exception {

        NiFiApiClient nifiApiClient = Mockito.mock(NiFiApiClient.class);

        ProcessGroupFlowEntity rootPGEntity = createEmptyProcessGroupFlowEntity();
        final FlowDTO flow = rootPGEntity.getProcessGroupFlow().getFlow();

        final ProcessorEntity pr0 = createProcessor(rootPGEntity, "org.apache.nifi.processors.standard.GenerateFlowFile");
        final ProcessorEntity pr1 = createProcessor(rootPGEntity, "org.apache.nifi.processors.standard.UpdateAttribute");
        final ProcessorEntity pr2 = createProcessor(rootPGEntity, "org.apache.nifi.processors.standard.LogAttribute");

        PortEntity inputPort1 = createInputPortEntity(flow, "input-1");
        PortEntity outputPort1 = createOutputPortEntity(flow, "output-1");

        connect(rootPGEntity, pr0, outputPort1);
        connect(rootPGEntity, inputPort1, pr1);
        connect(rootPGEntity, pr1, pr2);

        when(nifiApiClient.getProcessGroupFlow()).thenReturn(rootPGEntity);

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer(nifiApiClient);

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(atlasVariables);

        assertEquals(3, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final List<NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        assertEquals(2, paths.size());
        final Map<String, NiFiFlowPath> pathMap = paths.stream().collect(Collectors.toMap(p -> p.getId(), p -> p));
        final NiFiFlowPath pathA = pathMap.get(pr0.getId());
        final NiFiFlowPath pathB = pathMap.get(pr1.getId());

        assertEquals(1, pathA.getInputs().size()); // Obscure Ingress
        assertEquals(1, pathA.getOutputs().size());
        final AtlasObjectId output1 = pathA.getOutputs().iterator().next();
        assertEquals("nifi_output_port", output1.getTypeName());
        assertEquals(outputPort1.getId(), output1.getUniqueAttributes().get(ATTR_QUALIFIED_NAME));

        assertEquals(1, pathB.getInputs().size());
        assertEquals(0, pathB.getOutputs().size());
        final AtlasObjectId input1 = pathB.getInputs().iterator().next();
        assertEquals("nifi_input_port", input1.getTypeName());
        assertEquals(inputPort1.getId(), input1.getUniqueAttributes().get(ATTR_QUALIFIED_NAME));

    }

    private PortEntity createOutputPortEntity(FlowDTO flow, String name) {
        return createPortEntity(flow, name, "OUTPUT_PORT");
    }

    private PortEntity createInputPortEntity(FlowDTO flow, String name) {
        return createPortEntity(flow, name, "INPUT_PORT");
    }

    private PortEntity createPortEntity(FlowDTO flow, String name, String portType) {
        PortEntity port = new PortEntity();
        flow.getOutputPorts().add(port);

        port.setId(nextComponentId());
        port.setPortType(portType);

        PortDTO portDTO = new PortDTO();
        port.setComponent(portDTO);

        portDTO.setId(port.getId());
        portDTO.setName(name);
        portDTO.setComments(name + "-comment");
        return port;
    }

    @Test
    public void testRootGroupPortsAndChildProcessGroup() throws Exception {

        NiFiApiClient nifiApiClient = Mockito.mock(NiFiApiClient.class);

        ProcessGroupFlowEntity rootPGEntity = createEmptyProcessGroupFlowEntity();
        ProcessGroupFlowEntity childPG1 = createEmptyProcessGroupFlowEntity();
        ProcessGroupFlowEntity childPG2 = createEmptyProcessGroupFlowEntity();

        final FlowDTO flow = rootPGEntity.getProcessGroupFlow().getFlow();
        final ProcessGroupEntity childPG1Entity = new ProcessGroupEntity();
        childPG1Entity.setId(childPG1.getProcessGroupFlow().getId());
        final ProcessGroupEntity childPG2Entity = new ProcessGroupEntity();
        childPG2Entity.setId(childPG2.getProcessGroupFlow().getId());

        flow.getProcessGroups().add(childPG1Entity);
        flow.getProcessGroups().add(childPG2Entity);

        final ProcessorEntity pr0 = createProcessor(childPG1, "org.apache.nifi.processors.standard.GenerateFlowFile");
        final ProcessorEntity pr1 = createProcessor(childPG2, "org.apache.nifi.processors.standard.UpdateAttribute");
        final ProcessorEntity pr2 = createProcessor(childPG2, "org.apache.nifi.processors.standard.LogAttribute");

        PortEntity inputPort1 = createInputPortEntity(flow, "input-1");
        PortEntity outputPort1 = createOutputPortEntity(flow, "output-1");

        final PortEntity childOutput = createOutputPortEntity(childPG1.getProcessGroupFlow().getFlow(), "child-output");
        final PortEntity childInput = createOutputPortEntity(childPG2.getProcessGroupFlow().getFlow(), "child-input");

        // From GenerateFlowFile in a child pg to a root group output port.
        connect(childPG1, pr0, childOutput);
        connect(childPG1, childOutput, rootPGEntity, outputPort1);

        // From a root group input port to an input port within a child port then connects to processor.
        connect(rootPGEntity, inputPort1, childPG2, childInput);
        connect(childPG2, childInput, pr1);
        connect(childPG2, pr1, pr2);

        final Map<String, ProcessGroupFlowEntity> processGroups = new HashMap<>();
        processGroups.put(rootPGEntity.getProcessGroupFlow().getId(), rootPGEntity);
        processGroups.put(childPG1.getProcessGroupFlow().getId(), childPG1);
        processGroups.put(childPG2.getProcessGroupFlow().getId(), childPG2);

        when(nifiApiClient.getProcessGroupFlow()).thenReturn(rootPGEntity);
        doAnswer(invocation -> processGroups.get(invocation.getArgumentAt(0, String.class)))
                .when(nifiApiClient).getProcessGroupFlow(anyString());

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer(nifiApiClient);

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(atlasVariables);
        nifiFlow.dump();

        assertEquals(3, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final List<NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        assertEquals(2, paths.size());
        final Map<String, NiFiFlowPath> pathMap = paths.stream().collect(Collectors.toMap(p -> p.getId(), p -> p));
        final NiFiFlowPath pathA = pathMap.get(pr0.getId());
        final NiFiFlowPath pathB = pathMap.get(pr1.getId());

        assertEquals(1, pathA.getInputs().size()); // Obscure Ingress
        assertEquals(1, pathA.getOutputs().size());
        final AtlasObjectId output1 = pathA.getOutputs().iterator().next();
        assertEquals("nifi_output_port", output1.getTypeName());
        assertEquals(outputPort1.getId(), output1.getUniqueAttributes().get(ATTR_QUALIFIED_NAME));

        assertEquals(1, pathB.getInputs().size());
        assertEquals(0, pathB.getOutputs().size());
        final AtlasObjectId input1 = pathB.getInputs().iterator().next();
        assertEquals("nifi_input_port", input1.getTypeName());
        assertEquals(inputPort1.getId(), input1.getUniqueAttributes().get(ATTR_QUALIFIED_NAME));

    }

}

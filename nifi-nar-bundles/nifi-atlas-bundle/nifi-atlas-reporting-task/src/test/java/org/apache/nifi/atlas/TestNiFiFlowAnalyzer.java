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
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.Tuple;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;
import static org.apache.nifi.atlas.reporting.AtlasNiFiFlowLineage.ATLAS_NIFI_URL;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.when;

/**
 * Test {@link NiFiFlowAnalyzer} with simple mock code.
 * More complex and detailed tests are available in {@link org.apache.nifi.atlas.reporting.ITAtlasNiFiFlowLineage}.
 */
public class TestNiFiFlowAnalyzer {

    private static final MockPropertyValue NIFI_URL = new MockPropertyValue("http://localhost:8080/nifi");
    private int componentId = 0;

    @Before
    public void before() throws Exception {
        componentId = 0;
    }

    private ProcessGroupStatus createEmptyProcessGroupStatus() {
        final ProcessGroupStatus processGroupStatus = new ProcessGroupStatus();

        processGroupStatus.setId(nextComponentId());
        processGroupStatus.setName("Flow name");

        return processGroupStatus;
    }

    @Test
    public void testEmptyFlow() throws Exception {

        ReportingContext reportingContext = Mockito.mock(ReportingContext.class);
        EventAccess eventAccess = Mockito.mock(EventAccess.class);

        ProcessGroupStatus rootPG = createEmptyProcessGroupStatus();

        when(reportingContext.getEventAccess()).thenReturn(eventAccess);
        when(reportingContext.getProperty(ATLAS_NIFI_URL)).thenReturn(NIFI_URL);
        when(eventAccess.getGroupStatus(matches("root"))).thenReturn(rootPG);

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(reportingContext);

        assertEquals("Flow name", nifiFlow.getFlowName());
    }

    private ProcessorStatus createProcessor(ProcessGroupStatus pgStatus, String type) {
        final ProcessorStatus processor = new ProcessorStatus();
        processor.setName(type);
        processor.setId(nextComponentId());
        processor.setGroupId(pgStatus.getId());
        pgStatus.getProcessorStatus().add(processor);

        return  processor;
    }

    private String nextComponentId() {
        return String.format("1234-5678-0000-%04d", componentId++);
    }

    private void connect(ProcessGroupStatus pg0, Object o0, Object o1) {
        Function<Object, Tuple<String, String>> toTupple = o -> {
            Tuple<String, String> comp;
            if (o instanceof ProcessorStatus) {
                ProcessorStatus p = (ProcessorStatus) o;
                comp = new Tuple<>(p.getId(), p.getName());
            } else if (o instanceof PortStatus) {
                PortStatus p = (PortStatus) o;
                comp = new Tuple<>(p.getId(), p.getName());
            } else {
                throw new IllegalArgumentException("Not supported");
            }
            return comp;
        };
        connect(pg0, toTupple.apply(o0), toTupple.apply(o1));
    }

    private void connect(ProcessGroupStatus pg0, Tuple<String, String> comp0, Tuple<String, String> comp1) {
        ConnectionStatus conn = new ConnectionStatus();
        conn.setId(nextComponentId());
        conn.setGroupId(pg0.getId());

        conn.setSourceId(comp0.getKey());
        conn.setSourceName(comp0.getValue());

        conn.setDestinationId(comp1.getKey());
        conn.setDestinationName(comp1.getValue());

        pg0.getConnectionStatus().add(conn);
    }

    @Test
    public void testSingleProcessor() throws Exception {

        ReportingContext reportingContext = Mockito.mock(ReportingContext.class);
        EventAccess eventAccess = Mockito.mock(EventAccess.class);

        ProcessGroupStatus rootPG = createEmptyProcessGroupStatus();

        when(reportingContext.getProperty(ATLAS_NIFI_URL)).thenReturn(NIFI_URL);
        when(reportingContext.getEventAccess()).thenReturn(eventAccess);
        when(eventAccess.getGroupStatus(matches("root"))).thenReturn(rootPG);

        final ProcessorStatus pr0 = createProcessor(rootPG, "GenerateFlowFile");


        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(reportingContext);

        assertEquals(1, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final List<NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        assertEquals(2, paths.size());

        // root path
        final NiFiFlowPath path0 = paths.get(0);
        assertEquals(rootPG.getId(), path0.getId());
        assertEquals(rootPG.getId(), path0.getGroupId());

        // first path
        final NiFiFlowPath path1 = paths.get(1);
        assertEquals(path1.getId(), path1.getProcessorIds().get(0));
        assertEquals(rootPG.getId(), path1.getGroupId());

        // Should be able to find a path from a given processor GUID.
        final NiFiFlowPath pathForPr0 = nifiFlow.findPath(pr0.getId());
        assertEquals(path1, pathForPr0);
    }


    @Test
    public void testProcessorsWithinSinglePath() throws Exception {

        ReportingContext reportingContext = Mockito.mock(ReportingContext.class);
        EventAccess eventAccess = Mockito.mock(EventAccess.class);

        ProcessGroupStatus rootPG = createEmptyProcessGroupStatus();

        when(reportingContext.getProperty(ATLAS_NIFI_URL)).thenReturn(NIFI_URL);
        when(reportingContext.getEventAccess()).thenReturn(eventAccess);
        when(eventAccess.getGroupStatus(matches("root"))).thenReturn(rootPG);

        final ProcessorStatus pr0 = createProcessor(rootPG, "GenerateFlowFile");
        final ProcessorStatus pr1 = createProcessor(rootPG, "UpdateAttribute");

        connect(rootPG, pr0, pr1);

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(reportingContext);

        assertEquals(2, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final List<NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        assertEquals(2, paths.size());

        // Should be able to find a path from a given processor GUID.
        final NiFiFlowPath pathForPr0 = nifiFlow.findPath(pr0.getId());
        final NiFiFlowPath pathForPr1 = nifiFlow.findPath(pr1.getId());
        final NiFiFlowPath path1 = paths.get(1);
        assertEquals(path1, pathForPr0);
        assertEquals(path1, pathForPr1);
    }

    @Test
    public void testMultiPaths() throws Exception {

        ReportingContext reportingContext = Mockito.mock(ReportingContext.class);
        EventAccess eventAccess = Mockito.mock(EventAccess.class);

        ProcessGroupStatus rootPG = createEmptyProcessGroupStatus();

        when(reportingContext.getProperty(ATLAS_NIFI_URL)).thenReturn(NIFI_URL);
        when(reportingContext.getEventAccess()).thenReturn(eventAccess);
        when(eventAccess.getGroupStatus(matches("root"))).thenReturn(rootPG);


        final ProcessorStatus pr0 = createProcessor(rootPG, "GenerateFlowFile");
        final ProcessorStatus pr1 = createProcessor(rootPG, "UpdateAttribute");
        final ProcessorStatus pr2 = createProcessor(rootPG, "ListenTCP");
        final ProcessorStatus pr3 = createProcessor(rootPG, "LogAttribute");

        connect(rootPG, pr0, pr1);
        connect(rootPG, pr2, pr3);

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(reportingContext);

        assertEquals(4, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final List<NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        assertEquals(3, paths.size());

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

        ReportingContext reportingContext = Mockito.mock(ReportingContext.class);
        EventAccess eventAccess = Mockito.mock(EventAccess.class);

        ProcessGroupStatus rootPG = createEmptyProcessGroupStatus();

        when(reportingContext.getProperty(ATLAS_NIFI_URL)).thenReturn(NIFI_URL);
        when(reportingContext.getEventAccess()).thenReturn(eventAccess);
        when(eventAccess.getGroupStatus(matches("root"))).thenReturn(rootPG);

        final ProcessorStatus pr0 = createProcessor(rootPG, "org.apache.nifi.processors.standard.GenerateFlowFile");
        final ProcessorStatus pr1 = createProcessor(rootPG, "org.apache.nifi.processors.standard.UpdateAttribute");
        final ProcessorStatus pr2 = createProcessor(rootPG, "org.apache.nifi.processors.standard.ListenTCP");
        final ProcessorStatus pr3 = createProcessor(rootPG, "org.apache.nifi.processors.standard.LogAttribute");

        // Result should be as follows:
        // pathA = 0 -> 1 (-> 3)
        // pathB = 2 (-> 3)
        // pathC = 3
        connect(rootPG, pr0, pr1);
        connect(rootPG, pr1, pr3);
        connect(rootPG, pr2, pr3);

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(reportingContext);

        assertEquals(4, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final List<NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        assertEquals(4, paths.size());

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
    public void testTimeFormat() {
        final Date now = new Date();
        System.out.println(String.format("%tFT%tT.000%tz", now, now, now));

        String nows = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now());
        System.out.println(nows);
    }

}

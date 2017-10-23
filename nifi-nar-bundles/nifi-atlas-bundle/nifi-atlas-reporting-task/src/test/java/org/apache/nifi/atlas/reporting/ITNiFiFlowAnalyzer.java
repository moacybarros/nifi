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
package org.apache.nifi.atlas.reporting;

import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.nifi.atlas.NiFiAtlasClient;
import org.apache.nifi.atlas.emulator.AtlasAPIV2ServerEmulator;
import org.apache.nifi.atlas.emulator.Lineage;
import org.apache.nifi.atlas.emulator.Node;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.provenance.lineage.ComputeLineageResult;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.lineage.EdgeNode;
import org.apache.nifi.provenance.lineage.EventNode;
import org.apache.nifi.provenance.lineage.LineageEdge;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockPropertyValue;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Stack;
import java.util.function.BiConsumer;

import static org.apache.nifi.atlas.reporting.AtlasNiFiFlowLineage.ATLAS_NIFI_URL;
import static org.apache.nifi.atlas.reporting.AtlasNiFiFlowLineage.ATLAS_URLS;
import static org.apache.nifi.atlas.reporting.SimpleProvenanceRecord.pr;
import static org.apache.nifi.provenance.ProvenanceEventType.CREATE;
import static org.apache.nifi.provenance.ProvenanceEventType.RECEIVE;
import static org.apache.nifi.provenance.ProvenanceEventType.SEND;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ITNiFiFlowAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(ITNiFiFlowAnalyzer.class);

    private NiFiAtlasClient atlasClient;

    @Before
    public void setup() throws Exception {

        atlasClient = NiFiAtlasClient.getInstance();
        // Add your atlas server ip address into /etc/hosts as atlas.example.com
        atlasClient.initialize(new String[]{"http://atlas.example.com:21000/"}, "admin", "admin", null);

        final Properties atlasProperties = new Properties();
        try (InputStream in = ITNiFiFlowAnalyzer.class.getResourceAsStream("/atlas-application.properties")) {
            atlasProperties.load(in);
        }

    }

    /**
     * Load template file using the test method name.
     */
    private ProcessGroupStatus loadTemplate() {
        final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        return loadTemplate(stackTrace[2].getMethodName().substring(4));
    }

    private ProcessGroupStatus loadTemplate(String name) {

        final SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
        final SAXParser saxParser;
        try {
            saxParser = saxParserFactory.newSAXParser();
        } catch (ParserConfigurationException|SAXException e) {
            throw new RuntimeException("Failed to create a SAX parser", e);
        }

        final XMLReader xmlReader;
        try {
            xmlReader = saxParser.getXMLReader();
        } catch (SAXException e) {
            throw new RuntimeException("Failed to create a XML reader", e);
        }

        final String template = ITNiFiFlowAnalyzer.class.getResource("/flow-templates/" + name + ".xml").getPath();
        final TemplateContentHander handler = new TemplateContentHander(name);
        xmlReader.setContentHandler(handler);
        try {
            xmlReader.parse(template);
        } catch (IOException|SAXException e) {
            throw new RuntimeException("Failed to parse template", e);
        }

        return handler.getRootProcessGroupStatus();
    }

    private static class TemplateContentHander implements ContentHandler {

        private static class Context {
            private boolean isConnectionSource;
            private boolean isConnectionDestination;
            private boolean isRemoteProcessGroup;
            private Stack<String> stack = new Stack<>();
        }

        private final Map<Class, BiConsumer<Object, String>> nameSetters = new HashMap<>();
        private final Map<Class, BiConsumer<Object, String>> idSetters = new HashMap<>();
        private final Map<String, Map<Class, BiConsumer<Object, String>>> setters = new HashMap<>();
        private final Context context = new Context();

        private BiConsumer<Object, String> s(String tag, BiConsumer<Object, String> setter) {
            return (o, s) -> {
                // Only apply the function when the element is the first level child.
                // In order to avoid different 'name', 'id' or other common tags overwriting values.
                if (tag.equals(context.stack.get(context.stack.size() - 2))) {
                    setter.accept(o, s);
                }
            };
        }

        public TemplateContentHander(String name) {
            rootPgStatus = new ProcessGroupStatus();
            rootPgStatus.setId(name);
            rootPgStatus.setName(name);
            pgStatus = rootPgStatus;
            current = rootPgStatus;
            pgStack.push(rootPgStatus);

            setters.put("id", idSetters);
            setters.put("name", nameSetters);

            idSetters.put(ProcessGroupStatus.class, s("processGroups",
                    (o, id) -> ((ProcessGroupStatus) o).setId(id)));
            idSetters.put(ProcessorStatus.class, s("processors",
                    (o, id) -> ((ProcessorStatus) o).setId(id)));

            idSetters.put(PortStatus.class, (o, id) -> ((PortStatus) o).setId(id));

            idSetters.put(ConnectionStatus.class, (o, id) -> {
                if (context.isConnectionSource) {
                    ((ConnectionStatus) o).setSourceId(id);
                } else if (context.isConnectionDestination) {
                    ((ConnectionStatus) o).setDestinationId(id);
                } else {
                    ((ConnectionStatus) o).setId(id);
                }
            });

            nameSetters.put(ProcessGroupStatus.class, s("processGroups",
                    (o, n) -> ((ProcessGroupStatus) o).setName(n)));

            nameSetters.put(ProcessorStatus.class, s("processors",
                    (o, n) -> ((ProcessorStatus) o).setName(n)));

            nameSetters.put(PortStatus.class, (o, n) -> ((PortStatus) o).setName(n));

            nameSetters.put(ConnectionStatus.class, s("connections",
                    (o, n) -> ((ConnectionStatus) o).setName(n)));
        }

        private ProcessGroupStatus rootPgStatus;
        private ProcessGroupStatus parentPgStatus;
        private Stack<ProcessGroupStatus> pgStack = new Stack<>();
        private ProcessGroupStatus pgStatus;
        private ProcessorStatus processorStatus;
        private PortStatus portStatus;
        private ConnectionStatus connectionStatus;
        private Object current;
        private StringBuffer stringBuffer;
        private Map<String, String> componentNames = new HashMap<>();

        public ProcessGroupStatus getRootProcessGroupStatus() {
            return rootPgStatus;
        }

        @Override
        public void setDocumentLocator(Locator locator) {

        }

        @Override
        public void startDocument() throws SAXException {
        }

        private void setConnectionNames(ProcessGroupStatus pg) {
            pg.getConnectionStatus().forEach(c -> setConnectionName(c));
            pg.getProcessGroupStatus().forEach(child -> setConnectionNames(child));
        }

        private void setConnectionName(ConnectionStatus c) {
            if (c.getSourceName() == null || c.getSourceName().isEmpty()) {
                c.setSourceName(componentNames.get(c.getSourceId()));
            }
            if (c.getDestinationName() == null || c.getDestinationName().isEmpty()) {
                c.setDestinationName(componentNames.get(c.getDestinationId()));
            }
        }

        @Override
        public void endDocument() throws SAXException {
            setConnectionNames(rootPgStatus);
            System.out.println("rootPgStatus=" + rootPgStatus);
        }

        @Override
        public void startPrefixMapping(String prefix, String uri) throws SAXException {

        }

        @Override
        public void endPrefixMapping(String prefix) throws SAXException {

        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            // Clear flags.
            stringBuffer = new StringBuffer();

            switch (qName) {
                case "processGroups":
                    if (pgStatus != null) {
                        pgStack.push(pgStatus);
                    }
                    parentPgStatus = pgStatus;
                    pgStatus = new ProcessGroupStatus();
                    current = pgStatus;
                    if (parentPgStatus != null) {
                        parentPgStatus.getProcessGroupStatus().add(pgStatus);
                    }
                    break;

                case "processors":
                    processorStatus = new ProcessorStatus();
                    current = processorStatus;
                    pgStatus.getProcessorStatus().add(processorStatus);
                    break;

                case "inputPorts":
                case "outputPorts":
                    portStatus = new PortStatus();
                    current = portStatus;
                    if (!context.isRemoteProcessGroup) {
                        ("inputPorts".equals(qName)
                                ? pgStatus.getInputPortStatus()
                                : pgStatus.getOutputPortStatus())
                                .add(portStatus);
                    }
                    break;

                case "connections":
                    connectionStatus = new ConnectionStatus();
                    current = connectionStatus;
                    pgStatus.getConnectionStatus().add(connectionStatus);
                    context.isConnectionSource = false;
                    context.isConnectionDestination = false;
                    break;

                case "source":
                    if (current instanceof ConnectionStatus) {
                        context.isConnectionSource = true;
                    }
                    break;
                case "destination":
                    if (current instanceof ConnectionStatus) {
                        context.isConnectionDestination = true;
                    }
                    break;

                case "remoteProcessGroups":
                    context.isRemoteProcessGroup = true;
                    break;
            }
            context.stack.push(qName);

        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            switch (qName) {
                case "processGroups":
                    // At this point pgStatus has id assigned. Set group id of each component within this pg.
                    pgStatus.getProcessorStatus().forEach(s -> s.setGroupId(pgStatus.getId()));
                    pgStatus.getInputPortStatus().forEach(s -> s.setGroupId(pgStatus.getId()));
                    pgStatus.getOutputPortStatus().forEach(s -> s.setGroupId(pgStatus.getId()));
                    pgStatus.getConnectionStatus().forEach(s -> s.setGroupId(pgStatus.getId()));

                    // Put the previous ProcessGroup back to current.
                    pgStatus = pgStack.isEmpty() ? null : pgStack.pop();
                    current = pgStatus;
                    break;
                case "processors":
                case "connections":
                    current = pgStatus;
                    break;
                case "inputPorts":
                case "outputPorts":
                    current = pgStatus;
                    if (context.isRemoteProcessGroup) {
                        componentNames.put(portStatus.getId(), portStatus.getName());
                    }
                    break;
                case "id":
                case "name":
                    if (current != null) {
                        final BiConsumer<Object, String> setter = setters.get(qName).get(current.getClass());
                        if (setter == null) {
                            throw new RuntimeException(qName + " setter was not found: " + current.getClass());
                        }
                        setter.accept(current, stringBuffer.toString());
                    }
                    break;
                case "remoteProcessGroups":
                    context.isRemoteProcessGroup = false;
                    break;
            }
            context.stack.pop();
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
                stringBuffer.append(ch, start, length);
        }

        @Override
        public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {

        }

        @Override
        public void processingInstruction(String target, String data) throws SAXException {

        }

        @Override
        public void skippedEntity(String name) throws SAXException {

        }
    }

    private static String TARGET_ATLAS_URL = "http://localhost:21000";

    private Stack<Long> lineageSubmissionEventIds = new Stack<>();

    private void test(ProcessGroupStatus rootPgStatus, List<ProvenanceEventRecord> provenanceRecords)
            throws InitializationException, IOException {
        test(rootPgStatus, provenanceRecords, Collections.emptyMap());
    }

    private void test(ProcessGroupStatus rootPgStatus, List<ProvenanceEventRecord> provenanceRecords,
                      Map<Long, ComputeLineageResult> lineageResults)
            throws InitializationException, IOException {
        final AtlasNiFiFlowLineage reportingTask = new AtlasNiFiFlowLineage();
        final MockComponentLog logger = new MockComponentLog("reporting-task-id", reportingTask);
        final Map<PropertyDescriptor, String> properties = new HashMap<>();

        final ReportingInitializationContext initializationContext = mock(ReportingInitializationContext.class);
        when(initializationContext.getLogger()).thenReturn(logger);
        final ConfigurationContext configurationContext = new MockConfigurationContext(properties, null);
        final ValidationContext validationContext = mock(ValidationContext.class);
        when(validationContext.getProperty(any())).then(invocation -> new MockPropertyValue(properties.get(invocation.getArguments()[0])));
        final ReportingContext reportingContext = mock(ReportingContext.class);
        final MockStateManager stateManager = new MockStateManager(reportingTask);
        final EventAccess eventAccess = mock(EventAccess.class);
        when(reportingContext.getProperties()).thenReturn(properties);
        when(reportingContext.getProperty(any())).then(invocation -> new MockPropertyValue(properties.get(invocation.getArguments()[0])));
        when(reportingContext.getStateManager()).thenReturn(stateManager);
        when(reportingContext.getEventAccess()).thenReturn(eventAccess);
        when(eventAccess.getGroupStatus(eq("root"))).thenReturn(rootPgStatus);

        final ProvenanceRepository provenanceRepository = mock(ProvenanceRepository.class);
        when(eventAccess.getProvenanceRepository()).thenReturn(provenanceRepository);
        when(eventAccess.getProvenanceEvents(eq(-1L), anyInt())).thenReturn(provenanceRecords);
        when(provenanceRepository.getMaxEventId()).thenReturn((long) provenanceRecords.size() - 1);
        when(provenanceRepository.getEvent(anyLong())).then(invocation -> provenanceRecords.get(((Long) invocation.getArguments()[0]).intValue()));

        // To mock this async method invocations, keep the requested event ids in a stack.
        final ComputeLineageSubmission lineageSubmission = mock(ComputeLineageSubmission.class);
        when(provenanceRepository.submitLineageComputation(anyLong(), any())).thenAnswer(invocation -> {
            lineageSubmissionEventIds.push((Long) invocation.getArguments()[0]);
            return lineageSubmission;
        });
        when(lineageSubmission.getResult()).then(invocation -> lineageResults.get(lineageSubmissionEventIds.pop()));

        properties.put(ATLAS_NIFI_URL, "http://localhost:8080/nifi");
        properties.put(ATLAS_URLS, TARGET_ATLAS_URL);
        properties.put(new PropertyDescriptor.Builder().name("hostnamePattern.example").dynamic(true).build(), ".*");


        reportingTask.initialize(initializationContext);
        reportingTask.validate(validationContext);
        reportingTask.setup(configurationContext);
        reportingTask.onTrigger(reportingContext);
        reportingTask.onUnscheduled();
    }

    private boolean useEmbeddedEmulator;
    private AtlasAPIV2ServerEmulator atlasAPIServer;

    public ITNiFiFlowAnalyzer() {
        useEmbeddedEmulator = Boolean.valueOf(System.getenv("useEmbeddedEmulator"));
        if (useEmbeddedEmulator) {
            atlasAPIServer = new AtlasAPIV2ServerEmulator();
        }
    }

    @Before
    public void startEmulator() throws Exception {
        if (useEmbeddedEmulator) {
            atlasAPIServer.start();
        } else {
            // Clear existing entities.
            URL url = new URL(TARGET_ATLAS_URL + "/api/atlas/v2/entity/bulk/");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("DELETE");
            conn.connect();
            conn.getResponseCode();
            conn.disconnect();
        }
    }

    @After
    public void stopEmulator() throws Exception {
        if (useEmbeddedEmulator) {
            atlasAPIServer.stop();
        }
    }

    private static class ProvenanceRecords extends ArrayList<ProvenanceEventRecord> {
        @Override
        public boolean add(ProvenanceEventRecord record) {
            ((SimpleProvenanceRecord) record).setEventId(size());
            return super.add(record);
        }
    }


    private Lineage getLineage() throws Exception {
        final URL url = new URL("http://localhost:21000/api/atlas/v2/debug/lineage/");
        try (InputStream in = url.openStream()) {
            Lineage lineage = new ObjectMapper().reader().withType(Lineage.class).readValue(in);
            return lineage;
        }
    }

    private void waitNotificationsGetDelivered() throws InterruptedException {
        Thread.sleep(3_000);
    }

    @Test
    public void testSimplestPath() throws Exception {
        final ProcessGroupStatus rootPgStatus = loadTemplate("SimplestFlowPath");
        final ProvenanceRecords prs = new ProvenanceRecords();
        test(rootPgStatus, prs);

        final Lineage lineage = getLineage();
        lineage.assertLink("nifi_flow", "SimplestFlowPath", "SimplestFlowPath",
                "nifi_flow_path", "GenerateFlowFile, LogAttribute", "d270e6f0-c5e0-38b9-0000-000000000000");
    }

    @Test
    public void testSingleFlowPath() throws Exception {
        final ProcessGroupStatus rootPgStatus = loadTemplate("SingleFlowPath");
        final ProvenanceRecords prs = new ProvenanceRecords();
        prs.add(pr("2e9a2852-228f-379b-0000-000000000000", "ConsumeKafka_0_11", RECEIVE, "PLAINTEXT://0.kafka.example.com:6667/topic-a"));
        prs.add(pr("5a56149a-d82a-3242-0000-000000000000", "PublishKafka_0_11", SEND, "PLAINTEXT://0.kafka.example.com:6667/topic-b"));
        test(rootPgStatus, prs);

        waitNotificationsGetDelivered();

        final Lineage lineage = getLineage();
        final Node flow = lineage.findNode("nifi_flow", "SingleFlowPath", "SingleFlowPath");
        final Node path = lineage.findNode("nifi_flow_path",
                "ConsumeKafka_0_11, UpdateAttribute, ConvertJSONToSQL, PutSQL, PublishKafka_0_11",
                "2e9a2852-228f-379b-0000-000000000000");
        final Node topicA = lineage.findNode("kafka_topic", "topic-a@example");
        final Node topicB = lineage.findNode("kafka_topic", "topic-b@example");
        lineage.assertLink(flow, path);
        lineage.assertLink(topicA, path);
        lineage.assertLink(path, topicB);
    }

    @Test
    public void testMultipleProcessGroups() throws Exception {
        final ProcessGroupStatus rootPgStatus = loadTemplate("MultipleProcessGroups");
        final ProvenanceRecords prs = new ProvenanceRecords();
        prs.add(pr("989dabb7-54b9-3c78-0000-000000000000", "ConsumeKafka_0_11", RECEIVE, "PLAINTEXT://0.kafka.example.com:6667/nifi-test"));
        prs.add(pr("767c7bd6-75e3-3f32-0000-000000000000", "PutHDFS", SEND, "hdfs://nn1.example.com:8020/user/nifi/5262553828219"));
        test(rootPgStatus, prs);

        waitNotificationsGetDelivered();

        final Lineage lineage = getLineage();

        final Node flow = lineage.findNode("nifi_flow", "MultipleProcessGroups", "MultipleProcessGroups");
        final Node path = lineage.findNode("nifi_flow_path",
                "ConsumeKafka_0_11, UpdateAttribute, PutHDFS",
                "989dabb7-54b9-3c78-0000-000000000000");
        final Node kafkaTopic = lineage.findNode("kafka_topic", "nifi-test@example");
        final Node hdfsPath = lineage.findNode("hdfs_path", "/user/nifi/5262553828219@example");
        lineage.assertLink(flow, path);
        lineage.assertLink(kafkaTopic, path);
        lineage.assertLink(path, hdfsPath);

    }

    private EdgeNode createEdge(ProvenanceRecords prs, int srcIdx, int tgtIdx) {
        // Generate C created a FlowFile
        final ProvenanceEventRecord srcR = prs.get(srcIdx);
        // Then Remote Input Port sent it
        final ProvenanceEventRecord tgtR = prs.get(tgtIdx);
        final EventNode src = new EventNode(srcR);
        final EventNode tgt = new EventNode(tgtR);
        final EdgeNode edge = new EdgeNode(srcR.getComponentType() + " to " + tgtR.getEventType(), src, tgt);
        return edge;
    }

    private ComputeLineageResult createLineage(ProvenanceRecords prs, int ... indices) throws InterruptedException {
        final ComputeLineageResult lineage = mock(ComputeLineageResult.class);
        when(lineage.awaitCompletion(anyLong(), any())).thenReturn(true);
        final List<LineageEdge> edges = new ArrayList<>();
        for (int i = 0; i < indices.length - 1; i++) {
            edges.add(createEdge(prs, indices[i], indices[i + 1]));
        }
        when(lineage.getEdges()).thenReturn(edges);
        return lineage;
    }

    /**
     * A client NiFi sends FlowFiles to a remote NiFi.
     */
    @Test
    public void testS2SSend() throws Exception {
        final ProcessGroupStatus rootPgStatus = loadTemplate("S2SSend");
        final ProvenanceRecords prs = new ProvenanceRecords();
        prs.add(pr("ca71e4d9-2a4f-3970-0000-000000000000", "Generate A", CREATE));
        prs.add(pr("c439cdca-e989-3491-0000-000000000000", "Generate C", CREATE));
        prs.add(pr("b775b657-5a5b-3708-0000-000000000000", "GetTwitter", CREATE));

        prs.add((pr("77919f59-533e-35a3-0000-000000000000", "Remote Input Port", SEND,
                "http://nifi.example.com:8080/nifi-api/data-transfer/input-ports" +
                        "/77919f59-533e-35a3-0000-000000000000/transactions/tx-1/flow-files")));

        prs.add((pr("77919f59-533e-35a3-0000-000000000000", "Remote Input Port", SEND,
                "http://nifi.example.com:8080/nifi-api/data-transfer/input-ports" +
                        "/77919f59-533e-35a3-0000-000000000000/transactions/tx-2/flow-files")));

        Map<Long, ComputeLineageResult> lineages = new HashMap<>();
        // Generate C created a FlowFile, then it's sent via S2S
        lineages.put(3L, createLineage(prs, 1, 3));
        // GetTwitter created a FlowFile, then it's sent via S2S
        lineages.put(4L, createLineage(prs, 2, 4));

        test(rootPgStatus, prs, lineages);

        waitNotificationsGetDelivered();

        final Lineage lineage = getLineage();

        final Node flow = lineage.findNode("nifi_flow", "S2SSend", "S2SSend");
        final Node pathA = lineage.findNode("nifi_flow_path", "Generate A", "ca71e4d9-2a4f-3970-0000-000000000000");
        final Node pathB = lineage.findNode("nifi_flow_path", "Generate B", "333255b6-eb02-3056-0000-000000000000");
        final Node pathC = lineage.findNode("nifi_flow_path", "Generate C", "c439cdca-e989-3491-0000-000000000000");
        final Node pathT = lineage.findNode("nifi_flow_path", "GetTwitter", "b775b657-5a5b-3708-0000-000000000000");
        final Node pathI = lineage.findNode("nifi_flow_path", "InactiveProcessor", "7033f311-ac68-3cab-0000-000000000000");
        // UpdateAttribute has multiple incoming paths, so it generates a queue to receive those.
        final Node queueU = lineage.findNode("nifi_queue", "queue", "c5392447-e9f1-33ad-0000-000000000000");
        final Node pathU = lineage.findNode("nifi_flow_path", "UpdateAttribute", "c5392447-e9f1-33ad-0000-000000000000");

        // These are starting paths.
        lineage.assertLink(flow, pathA);
        lineage.assertLink(flow, pathB);
        lineage.assertLink(flow, pathC);
        lineage.assertLink(flow, pathT);
        lineage.assertLink(flow, pathI);

        // Multiple paths connected to the same path.
        lineage.assertLink(pathB, queueU);
        lineage.assertLink(pathC, queueU);
        lineage.assertLink(queueU, pathU);

        // Generate C and GetTwitter have reported proper SEND lineage to the input port.
        final Node remoteInputPort = lineage.findNode("nifi_input_port", "input", "77919f59-533e-35a3-0000-000000000000");
        lineage.assertLink(pathC, remoteInputPort);
        lineage.assertLink(pathT, remoteInputPort);

        // nifi_data is created for each obscure input processor.
        final Node genA = lineage.findNode("nifi_data", "Generate A", "ca71e4d9-2a4f-3970-0000-000000000000");
        final Node genC = lineage.findNode("nifi_data", "Generate C", "c439cdca-e989-3491-0000-000000000000");
        final Node genT = lineage.findNode("nifi_data", "GetTwitter", "b775b657-5a5b-3708-0000-000000000000");
        lineage.assertLink(genA, pathA);
        lineage.assertLink(genC, pathC);
        lineage.assertLink(genT, pathT);

    }

    /**
     * A client NiFi gets FlowFiles from a remote NiFi.
     */
    @Test
    public void testS2SGet() throws Exception {
        final ProcessGroupStatus rootPgStatus = loadTemplate("S2SGet");
        final ProvenanceRecords prs = new ProvenanceRecords();
        prs.add(pr("392e7343-3950-329b-0000-000000000000", "Remote Output Port", RECEIVE,
                "http://nifi.example.com:8080/nifi-api/data-transfer/input-ports" +
                        "/392e7343-3950-329b-0000-000000000000/transactions/tx-1/flow-files"));

        test(rootPgStatus, prs);

        waitNotificationsGetDelivered();

        final Lineage lineage = getLineage();

        final Node flow = lineage.findNode("nifi_flow", "S2SGet", "S2SGet");
        final Node pathL = lineage.findNode("nifi_flow_path", "LogAttribute", "97cc5b27-22f3-3c3b-0000-000000000000");
        final Node pathP = lineage.findNode("nifi_flow_path", "PutFile", "4f3bfa4c-6427-3aac-0000-000000000000");
        final Node pathU = lineage.findNode("nifi_flow_path", "UpdateAttribute", "bb530e58-ee14-3cac-0000-000000000000");
        final Node remoteOutputPort = lineage.findNode("nifi_output_port", "output", "392e7343-3950-329b-0000-000000000000");

        lineage.assertLink(flow, pathL);
        lineage.assertLink(flow, pathP);
        lineage.assertLink(flow, pathU);

        lineage.assertLink(remoteOutputPort, pathL);
        lineage.assertLink(remoteOutputPort, pathP);
        lineage.assertLink(remoteOutputPort, pathU);

    }

    /**
     * A remote NiFi transfers FlowFiles to remote client NiFis.
     * This NiFi instance owns RootProcessGroup output port.
     */
    @Test
    public void testS2STransfer() throws Exception {
        final ProcessGroupStatus rootPgStatus = loadTemplate("S2STransfer");
        final ProvenanceRecords prs = new ProvenanceRecords();
        test(rootPgStatus, prs);

        waitNotificationsGetDelivered();

        final Lineage lineage = getLineage();

        final Node flow = lineage.findNode("nifi_flow", "S2STransfer", "S2STransfer");
        final Node path = lineage.findNode("nifi_flow_path", "GenerateFlowFile", "1b9f81db-a0fd-389a-0000-000000000000");
        final Node outputPort = lineage.findNode("nifi_output_port", "output", "392e7343-3950-329b-0000-000000000000");

        lineage.assertLink(flow, path);
        lineage.assertLink(path, outputPort);
    }

    /**
     * A remote NiFi receives FlowFiles from remote client NiFis.
     * This NiFi instance owns RootProcessGroup input port.
     */
    @Test
    public void testS2SReceive() throws Exception {
        final ProcessGroupStatus rootPgStatus = loadTemplate("S2SReceive");
        final ProvenanceRecords prs = new ProvenanceRecords();
        test(rootPgStatus, prs);

        waitNotificationsGetDelivered();

        final Lineage lineage = getLineage();

        final Node flow = lineage.findNode("nifi_flow", "S2SReceive", "S2SReceive");
        final Node path = lineage.findNode("nifi_flow_path", "UpdateAttribute", "67834454-5a13-3872-0000-000000000000");
        final Node inputPort = lineage.findNode("nifi_input_port", "input", "77919f59-533e-35a3-0000-000000000000");

        lineage.assertLink(flow, path);
        lineage.assertLink(flow, inputPort);

        lineage.assertLink(inputPort, path);
    }

    @Test
    public void testS2SReceiveAndSendCombination() throws Exception {
        testS2SReceive();
        testS2SSend();

        final Lineage lineage = getLineage();

        final Node remoteFlow = lineage.findNode("nifi_flow", "S2SReceive", "S2SReceive");
        final Node localFlow = lineage.findNode("nifi_flow", "S2SSend", "S2SSend");
        final Node inputPort = lineage.findNode("nifi_input_port", "input", "77919f59-533e-35a3-0000-000000000000");
        final Node pathC = lineage.findNode("nifi_flow_path", "Generate C", "c439cdca-e989-3491-0000-000000000000");
        final Node pathT = lineage.findNode("nifi_flow_path", "GetTwitter", "b775b657-5a5b-3708-0000-000000000000");

        // Remote flow owns the inputPort.
        lineage.assertLink(remoteFlow, inputPort);

        // These paths within local flow sends data to the remote flow through the remote input port.
        lineage.assertLink(localFlow, pathC);
        lineage.assertLink(localFlow, pathT);
        lineage.assertLink(pathC, inputPort);
        lineage.assertLink(pathT, inputPort);

    }

    @Test
    public void testS2STransferAndGetCombination() throws Exception {
        testS2STransfer();
        testS2SGet();

        final Lineage lineage = getLineage();

        final Node remoteFlow = lineage.findNode("nifi_flow", "S2STransfer", "S2STransfer");
        final Node localFlow = lineage.findNode("nifi_flow", "S2SGet", "S2SGet");
        final Node remoteGen = lineage.findNode("nifi_flow_path", "GenerateFlowFile", "1b9f81db-a0fd-389a-0000-000000000000");
        final Node outputPort = lineage.findNode("nifi_output_port", "output", "392e7343-3950-329b-0000-000000000000");
        final Node pathL = lineage.findNode("nifi_flow_path", "LogAttribute", "97cc5b27-22f3-3c3b-0000-000000000000");
        final Node pathP = lineage.findNode("nifi_flow_path", "PutFile", "4f3bfa4c-6427-3aac-0000-000000000000");
        final Node pathU = lineage.findNode("nifi_flow_path", "UpdateAttribute", "bb530e58-ee14-3cac-0000-000000000000");

        // Remote flow owns the outputPort and transfer data generated by GenerateFlowFile.
        lineage.assertLink(remoteFlow, remoteGen);
        lineage.assertLink(remoteGen, outputPort);

        // These paths within local flow gets data from the remote flow through the remote output port.
        lineage.assertLink(localFlow, pathL);
        lineage.assertLink(localFlow, pathP);
        lineage.assertLink(localFlow, pathU);
        lineage.assertLink(outputPort, pathL);
        lineage.assertLink(outputPort, pathP);
        lineage.assertLink(outputPort, pathU);

    }

}

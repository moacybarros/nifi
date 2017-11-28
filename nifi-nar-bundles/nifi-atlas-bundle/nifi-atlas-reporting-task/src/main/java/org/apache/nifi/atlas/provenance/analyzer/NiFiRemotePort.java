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
package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.lineage.ComputeLineageResult;
import org.apache.nifi.provenance.lineage.LineageNode;
import org.apache.nifi.provenance.lineage.LineageNodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_INPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_OUTPUT_PORT;

/**
 * Analyze a transit URI as a NiFi Site-to-Site remote input/output port.
 * <li>qualifiedName=remotePortGUID (example: 35dbc0ab-015e-1000-144c-a8d71255027d)
 * <li>name=portName (example: input)
 */
public class NiFiRemotePort extends AbstractNiFiProvenanceEventAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(NiFiRemotePort.class);

    private static final Pattern URL_REGEX = Pattern.compile(".*/nifi-api/data-transfer/(in|out)put-ports/([^/]+)/transactions/.*");

    /**
     * This analyzer queries NiFi provenance event to figure out connecting processors to a RemotePort.
     * Then create a reference between the connecting processors (Process) and the RemotePort (DataSet).
     *
     * NOTE: Ideally RemotePort should be represented as a Process entity in Atlas lineage.
     *
     * Another challenge is to know whether a component is RemotePort, by only looking at the ConnectionStatus.
     * That is impossible because ConnectionStatus does not have source and target type.
     * ConnectionStatus only knows source id and target id.
     * >> This can be overcome by looking at the connection name?? Source name and Destination name.
     */
    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {

        if (!ProvenanceEventType.SEND.equals(event.getEventType())
                && !ProvenanceEventType.RECEIVE.equals(event.getEventType())) {
            return null;
        }

        final boolean isRemoteInputPort = event.getComponentType().equals("Remote Input Port");
        final String type = isRemoteInputPort ? TYPE_NIFI_INPUT_PORT : TYPE_NIFI_OUTPUT_PORT;

        final String remotePortId = event.getComponentId();

        // TODO: remotePortId is not equal to remotePortTargetId. Need to extract from transit URL
        final URI uri = parseUri(event.getTransitUri());
        final Matcher uriMatcher = URL_REGEX.matcher(uri.getPath());
        if (!uriMatcher.matches()) {
            logger.warn("Unexpected transit URI: {}, {}", new Object[]{uri, event});
            return null;
        }
        final String remotePortTargetId = uriMatcher.group(2);

        // Find connections that connects to/from the remote port.
        final List<ConnectionStatus> connections = isRemoteInputPort
                ? context.findConnectionTo(remotePortId)
                : context.findConnectionFrom(remotePortId);
        if (connections == null || connections.isEmpty()) {
            logger.warn("Connection was not found: {}", new Object[]{event});
            return null;
        }

        // The name of remote port can be retrieved from any connection, use the first one.
        final ConnectionStatus connection = connections.get(0);
        final Referenceable ref = new Referenceable(type);
        ref.set(ATTR_NAME, isRemoteInputPort ? connection.getDestinationName() : connection.getSourceName());
        ref.set(ATTR_QUALIFIED_NAME, remotePortTargetId);

        return singleDataSetRef(event.getComponentId(), event.getEventType(), ref);
    }

    @Override
    public String targetComponentTypePattern() {
        return "^Remote (In|Out)put Port$";
    }
}

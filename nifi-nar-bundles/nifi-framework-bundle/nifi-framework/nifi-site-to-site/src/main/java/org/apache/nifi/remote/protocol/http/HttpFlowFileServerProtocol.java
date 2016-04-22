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
package org.apache.nifi.remote.protocol.http;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.codec.StandardFlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.protocol.AbstractFlowFileServerProtocol;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.HandshakenProperties;
import org.apache.nifi.remote.protocol.RequestType;
import org.apache.nifi.remote.protocol.socket.Response;
import org.apache.nifi.remote.protocol.socket.ResponseCode;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.StopWatch;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class HttpFlowFileServerProtocol extends AbstractFlowFileServerProtocol {

    public static final String RESOURCE_NAME = "HttpFlowFileProtocol";

    private final FlowFileCodec codec = new StandardFlowFileCodec();


    private final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(5, 4, 3, 2, 1);

    @Override
    public FlowFileCodec negotiateCodec(final Peer peer) throws IOException {
        return codec;
    }

    @Override
    public FlowFileCodec getPreNegotiatedCodec() {
        return codec;
    }

    @Override
    protected HandshakenProperties doHandshake(Peer peer) throws IOException, HandshakeException {
        // TODO: implement handshake logic.
        HandshakenProperties confirmed = new HandshakenProperties();
        logger.debug("### Done handshake, confirmed=" + confirmed);
        return confirmed;
    }

    @Override
    protected void writeTransactionResponse(ResponseCode response, CommunicationsSession commsSession) throws IOException {
        switch (response) {
            case CONTINUE_TRANSACTION:
                logger.debug("### continue transaction... expecting more flow files.");
                break;
            case BAD_CHECKSUM:
                logger.debug("### received BAD_CHECKSUM.");
                break;
            case FINISH_TRANSACTION:
                logger.debug("### transaction finished... proceeding to Checksum.");
                break;
        }
    }

    @Override
    protected Response readTransactionResponse(CommunicationsSession commsSession) throws IOException {
        // TODO: return ResponseCode.CONFIRM_TRANSACTION
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ResponseCode.CONFIRM_TRANSACTION.writeResponse(new DataOutputStream(bos), "SKIP");
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        return Response.read(new DataInputStream(bis));
    }

    /*
    @Override
    public int transferFlowFiles(final Peer peer, final ProcessContext context, final ProcessSession session, final FlowFileCodec codec) throws IOException, ProtocolException {
        logger.debug("{} Sending FlowFiles to {}", this, peer);
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final InputStream is = commsSession.getInput().getInputStream();
        final OutputStream os = commsSession.getOutput().getOutputStream();
        String remoteDn = commsSession.getUserDn();
        if (remoteDn == null) {
            remoteDn = "none";
        }

        final StopWatch stopWatch = new StopWatch(true);
        long bytesSent = 0L;
        final Set<FlowFile> flowFilesSent = new HashSet<>();


        while(true){
            FlowFile flowFile = session.get();
            if (flowFile == null) {
                // we have no data to send. Notify the peer.
                logger.debug("{} No more data to send to {}", this, peer);
                break;
            }

            logger.debug("{} Sending {} to {}", new Object[]{this, flowFile, peer});

            final StopWatch transferWatch = new StopWatch(true);

            final FlowFile toSend = flowFile;
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    final DataPacket dataPacket = new StandardDataPacket(toSend.getAttributes(), in, toSend.getSize());
                    codec.encode(dataPacket, os);
                    os.flush();
                }
            });

            final long transmissionMillis = transferWatch.getElapsed(TimeUnit.MILLISECONDS);

            flowFilesSent.add(flowFile);
            bytesSent += flowFile.getSize();

            final String transitUri = (transitUriPrefix == null) ? peer.getUrl() : transitUriPrefix + flowFile.getAttribute(CoreAttributes.UUID.key());
            session.getProvenanceReporter().send(flowFile, transitUri, "Remote Host=" + peer.getHost() + ", Remote DN=" + remoteDn, transmissionMillis, false);
            session.remove(flowFile);
        }



        // TODO: Should we wait for receiving tx completed request from client? Otherwise we can't determine if the client successfully stored the flow file.
//        final Response transactionResponse;
//        try {
//            transactionResponse = Response.read(dis);
//        } catch (final IOException e) {
//            logger.error("{} Failed to receive a response from {} when expecting a TransactionFinished Indicator."
//                    + " It is unknown whether or not the peer successfully received/processed the data."
//                    + " Therefore, {} will be rolled back, possibly resulting in data duplication of {}",
//                    this, peer, session, flowFileDescription);
//            session.rollback();
//            throw e;
//        }

        // TODO: How to penalize?
//        logger.debug("{} received {} from {}", new Object[]{this, transactionResponse, peer});
//        if (transactionResponse.getCode() == ResponseCode.TRANSACTION_FINISHED_BUT_DESTINATION_FULL) {
//            peer.penalize(port.getIdentifier(), port.getYieldPeriod(TimeUnit.MILLISECONDS));
//        } else if (transactionResponse.getCode() != ResponseCode.TRANSACTION_FINISHED) {
//            throw new ProtocolException("After sending data, expected TRANSACTION_FINISHED response but got " + transactionResponse);
//        }

        final String flowFileDescription = flowFilesSent.size() < 20 ? flowFilesSent.toString() : flowFilesSent.size() + " FlowFiles";

        session.commit();

        stopWatch.stop();
        final String uploadDataRate = stopWatch.calculateDataRate(bytesSent);
        final long uploadMillis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
        final String dataSize = FormatUtils.formatDataSize(bytesSent);
        logger.info("{} Successfully sent {} ({}) to {} in {} milliseconds at a rate of {}", new Object[]{
                this, flowFileDescription, dataSize, peer, uploadMillis, uploadDataRate});

        return flowFilesSent.size();
    }

    */

    @Override
    public int receiveFlowFiles(final Peer peer, final ProcessContext context, final ProcessSession session, final FlowFileCodec codec) throws IOException, ProtocolException {
        logger.debug("{} receiving FlowFiles from {}", this, peer);

        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());
        String remoteDn = commsSession.getUserDn();
        if (remoteDn == null) {
            remoteDn = "none";
        }

        final StopWatch stopWatch = new StopWatch(true);
        long bytesReceived = 0L;
        final Set<FlowFile> flowFilesReceived = new HashSet<>();
        while(true){
            final DataPacket dataPacket = codec.decode(commsSession.getInput().getInputStream());
            if(dataPacket == null) {
                break;
            }
            FlowFile flowFile = handleIncomingDataPacket(peer, session, remoteDn, dataPacket);
            flowFilesReceived.add(flowFile);
            bytesReceived += flowFile.getSize();
        }

        // Commit the session so that we have persisted the data
        session.commit();

        // TODO: Do we need stuff like this?
        if (context.getAvailableRelationships().isEmpty()) {
            // Confirm that we received the data and the peer can now discard it but that the peer should not
            // send any more data for a bit
            logger.debug("{} Sending TRANSACTION_FINISHED_BUT_DESTINATION_FULL to {}", this, peer);
            ResponseCode.TRANSACTION_FINISHED_BUT_DESTINATION_FULL.writeResponse(dos);
        } else {
            // Confirm that we received the data and the peer can now discard it
            logger.debug("{} Sending TRANSACTION_FINISHED to {}", this, peer);
            ResponseCode.TRANSACTION_FINISHED.writeResponse(dos);
        }

        stopWatch.stop();

        // TODO: This logic is the same as Socket's.
        final String flowFileDescription = flowFilesReceived.size() < 20 ? flowFilesReceived.toString() : flowFilesReceived.size() + " FlowFiles";
        final String uploadDataRate = stopWatch.calculateDataRate(bytesReceived);
        final long uploadMillis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
        final String dataSize = FormatUtils.formatDataSize(bytesReceived);
        logger.info("{} Successfully received {} ({}) from {} in {} milliseconds at a rate of {}", new Object[]{
                this, flowFileDescription, dataSize, peer, uploadMillis, uploadDataRate});

        return 1;
    }

    // TODO: consolidate this logic with SocketFlowFileServerProtocol.
    private FlowFile handleIncomingDataPacket(Peer peer, ProcessSession session, String remoteDn, DataPacket dataPacket) {
        final long startNanos = System.nanoTime();

        FlowFile flowFile = session.create();
        flowFile = session.importFrom(dataPacket.getData(), flowFile);
        flowFile = session.putAllAttributes(flowFile, dataPacket.getAttributes());

        final long transferNanos = System.nanoTime() - startNanos;
        final long transferMillis = TimeUnit.MILLISECONDS.convert(transferNanos, TimeUnit.NANOSECONDS);
        final String sourceSystemFlowFileUuid = dataPacket.getAttributes().get(CoreAttributes.UUID.key());
        flowFile = session.putAttribute(flowFile, CoreAttributes.UUID.key(), UUID.randomUUID().toString());

        String transitUriPrefix = handshakenProperties.getTransitUriPrefix();
        final String transitUri = (transitUriPrefix == null) ? peer.getUrl() : transitUriPrefix + sourceSystemFlowFileUuid;
        session.getProvenanceReporter().receive(flowFile, transitUri, sourceSystemFlowFileUuid == null
                ? null : "urn:nifi:" + sourceSystemFlowFileUuid, "Remote Host=" + peer.getHost() + ", Remote DN=" + remoteDn, transferMillis);
        session.transfer(flowFile, Relationship.ANONYMOUS);
        return flowFile;
    }

    @Override
    public RequestType getRequestType(final Peer peer) throws IOException {
        return null;
    }

    @Override
    public VersionNegotiator getVersionNegotiator() {
        return versionNegotiator;
    }

    @Override
    public void sendPeerList(final Peer peer) throws IOException {
    }

    @Override
    public String getResourceName() {
        return RESOURCE_NAME;
    }

}

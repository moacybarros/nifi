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
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.cluster.NodeInformant;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.codec.HttpFlowFileCodec;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.RequestType;
import org.apache.nifi.remote.protocol.ServerProtocol;
import org.apache.nifi.remote.protocol.socket.Response;
import org.apache.nifi.remote.protocol.socket.ResponseCode;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

public class HttpFlowFileServerProtocol implements ServerProtocol {

    public static final String RESOURCE_NAME = "HttpFlowFileProtocol";

    private RootGroupPort port;
    private String transitUriPrefix = null;
    private final HttpFlowFileCodec codec = new HttpFlowFileCodec();

    private int requestedBatchCount = 0;
    private long requestedBatchBytes = 0L;
    private long requestedBatchNanos = 0L;
    private static final long DEFAULT_BATCH_NANOS = TimeUnit.SECONDS.toNanos(5L);

    private final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(5, 4, 3, 2, 1);
    private final Logger logger = LoggerFactory.getLogger(HttpFlowFileServerProtocol.class);

    public void setTransferredData(Map<String, String> attributes, int numBytes){
        codec.setAttributes(attributes);
        codec.setNumBytes(numBytes);
    }

    @Override
    public void setRootProcessGroup(final ProcessGroup group) {
    }

    @Override
    public void handshake(final Peer peer) throws IOException {
    }

    @Override
    public boolean isHandshakeSuccessful() {
        return true;
    }

    @Override
    public RootGroupPort getPort() {
        return port;
    }

    @Override
    public FlowFileCodec negotiateCodec(final Peer peer) throws IOException {
        return codec;
    }

    @Override
    public FlowFileCodec getPreNegotiatedCodec() {
        return codec;
    }

    @Override
    public int transferFlowFiles(final Peer peer, final ProcessContext context, final ProcessSession session, final FlowFileCodec codec) throws IOException, ProtocolException {
        logger.debug("{} Sending FlowFiles to {}", this, peer);
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());
        String remoteDn = commsSession.getUserDn();
        if (remoteDn == null) {
            remoteDn = "none";
        }

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            // we have no data to send. Notify the peer.
            logger.debug("{} No data to send to {}", this, peer);
            ResponseCode.NO_MORE_DATA.writeResponse(dos);
            return 0;
        }

        // we have data to send.
        logger.debug("{} Data is available to send to {}", this, peer);
        ResponseCode.MORE_DATA.writeResponse(dos);

        final StopWatch stopWatch = new StopWatch(true);
        long bytesSent = 0L;
        final Set<FlowFile> flowFilesSent = new HashSet<>();
        final CRC32 crc = new CRC32();

        // send data until we reach some batch size
        boolean continueTransaction = true;
        final long startNanos = System.nanoTime();
        String calculatedCRC = "";
        while (continueTransaction) {
            final OutputStream flowFileOutputStream = dos;
            logger.debug("{} Sending {} to {}", new Object[]{this, flowFile, peer});

            final CheckedOutputStream checkedOutputStream = new CheckedOutputStream(flowFileOutputStream, crc);

            final StopWatch transferWatch = new StopWatch(true);

            final FlowFile toSend = flowFile;
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    final DataPacket dataPacket = new StandardDataPacket(toSend.getAttributes(), in, toSend.getSize());
                    codec.encode(dataPacket, checkedOutputStream);
                }
            });

            final long transmissionMillis = transferWatch.getElapsed(TimeUnit.MILLISECONDS);

            flowFilesSent.add(flowFile);
            bytesSent += flowFile.getSize();

            final String transitUri = (transitUriPrefix == null) ? peer.getUrl() : transitUriPrefix + flowFile.getAttribute(CoreAttributes.UUID.key());
            session.getProvenanceReporter().send(flowFile, transitUri, "Remote Host=" + peer.getHost() + ", Remote DN=" + remoteDn, transmissionMillis, false);
            session.remove(flowFile);

            // determine if we should check for more data on queue.
            final long sendingNanos = System.nanoTime() - startNanos;
            boolean poll = true;
            if (sendingNanos >= requestedBatchNanos && requestedBatchNanos > 0L) {
                poll = false;
            }
            if (bytesSent >= requestedBatchBytes && requestedBatchBytes > 0L) {
                poll = false;
            }
            if (flowFilesSent.size() >= requestedBatchCount && requestedBatchCount > 0) {
                poll = false;
            }

            if (requestedBatchNanos == 0 && requestedBatchBytes == 0 && requestedBatchCount == 0) {
                poll = (sendingNanos < DEFAULT_BATCH_NANOS);
            }

            if (poll) {
                // we've not elapsed the requested sending duration, so get more data.
                flowFile = session.get();
            } else {
                flowFile = null;
            }

            continueTransaction = (flowFile != null);
            if (continueTransaction) {
                logger.debug("{} Sending ContinueTransaction indicator to {}", this, peer);
                ResponseCode.CONTINUE_TRANSACTION.writeResponse(dos);
            } else {
                logger.debug("{} Sending FinishTransaction indicator to {}", this, peer);
                ResponseCode.FINISH_TRANSACTION.writeResponse(dos);
                calculatedCRC = String.valueOf(checkedOutputStream.getChecksum().getValue());
            }
        }

        // we've sent a FINISH_TRANSACTION. Now we'll wait for the peer to send a 'Confirm Transaction' response
        final Response transactionConfirmationResponse = Response.read(dis);
        if (transactionConfirmationResponse.getCode() == ResponseCode.CONFIRM_TRANSACTION) {
            // Confirm Checksum and echo back the confirmation.
            logger.debug("{} Received {}  from {}", this, transactionConfirmationResponse, peer);
            final String receivedCRC = transactionConfirmationResponse.getMessage();

            if (versionNegotiator.getVersion() > 3) {
                if (!receivedCRC.equals(calculatedCRC)) {
                    ResponseCode.BAD_CHECKSUM.writeResponse(dos);
                    session.rollback();
                    throw new IOException(this + " Sent data to peer " + peer + " but calculated CRC32 Checksum as "
                            + calculatedCRC + " while peer calculated CRC32 Checksum as " + receivedCRC
                            + "; canceling transaction and rolling back session");
                }
            }

            ResponseCode.CONFIRM_TRANSACTION.writeResponse(dos, "");
        } else {
            throw new ProtocolException("Expected to receive 'Confirm Transaction' response from peer " + peer + " but received " + transactionConfirmationResponse);
        }

        final String flowFileDescription = flowFilesSent.size() < 20 ? flowFilesSent.toString() : flowFilesSent.size() + " FlowFiles";

        final Response transactionResponse;
        try {
            transactionResponse = Response.read(dis);
        } catch (final IOException e) {
            logger.error("{} Failed to receive a response from {} when expecting a TransactionFinished Indicator."
                    + " It is unknown whether or not the peer successfully received/processed the data."
                    + " Therefore, {} will be rolled back, possibly resulting in data duplication of {}",
                    this, peer, session, flowFileDescription);
            session.rollback();
            throw e;
        }

        logger.debug("{} received {} from {}", new Object[]{this, transactionResponse, peer});
        if (transactionResponse.getCode() == ResponseCode.TRANSACTION_FINISHED_BUT_DESTINATION_FULL) {
            peer.penalize(port.getIdentifier(), port.getYieldPeriod(TimeUnit.MILLISECONDS));
        } else if (transactionResponse.getCode() != ResponseCode.TRANSACTION_FINISHED) {
            throw new ProtocolException("After sending data, expected TRANSACTION_FINISHED response but got " + transactionResponse);
        }

        session.commit();

        stopWatch.stop();
        final String uploadDataRate = stopWatch.calculateDataRate(bytesSent);
        final long uploadMillis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
        final String dataSize = FormatUtils.formatDataSize(bytesSent);
        logger.info("{} Successfully sent {} ({}) to {} in {} milliseconds at a rate of {}", new Object[]{
            this, flowFileDescription, dataSize, peer, uploadMillis, uploadDataRate});

        return flowFilesSent.size();
    }

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

        // TODO : Pass DataPacket somehow.
        final DataPacket dataPacket = codec.decode(commsSession.getInput().getInputStream());
        FlowFile flowFile = handleIncomingDataPacket(peer, session, remoteDn, dataPacket);
        bytesReceived += flowFile.getSize();

        // Commit the session so that we have persisted the data
        session.commit();

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
        final String flowFileDescription = flowFile.toString();
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
    public void shutdown(final Peer peer) {
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public void sendPeerList(final Peer peer) throws IOException {
    }

    @Override
    public String getResourceName() {
        return RESOURCE_NAME;
    }

    @Override
    public void setNodeInformant(final NodeInformant nodeInformant) {
    }

    @Override
    public long getRequestExpiration() {
        return 0L;
    }

}

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
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.codec.StandardFlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.io.http.HttpCommunicationsSession;
import org.apache.nifi.remote.protocol.AbstractFlowFileServerProtocol;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.FlowFileTransaction;
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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class HttpFlowFileServerProtocol extends AbstractFlowFileServerProtocol {

    public static final String RESOURCE_NAME = "HttpFlowFileProtocol";

    private final FlowFileCodec codec = new StandardFlowFileCodec();
    private final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(5, 4, 3, 2, 1);

    private ConcurrentMap<String, FlowFileTransaction> txOnHold;

    public HttpFlowFileServerProtocol(ConcurrentMap<String, FlowFileTransaction> txOnHold){
        super();
        this.txOnHold = txOnHold;
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
    protected HandshakenProperties doHandshake(Peer peer) throws IOException, HandshakeException {
        // TODO: implement handshake logic.
        HandshakenProperties confirmed = new HandshakenProperties();
        logger.debug("### Done handshake, confirmed=" + confirmed);
        return confirmed;
    }

    @Override
    protected void writeTransactionResponse(ResponseCode response, CommunicationsSession commsSession) throws IOException {
        HttpCommunicationsSession commSession = (HttpCommunicationsSession) commsSession;
        switch (response) {
            case NO_MORE_DATA:
                // TODO: How can I return 204 here?
                logger.debug("### There's no data to send.");
                break;
            case CONTINUE_TRANSACTION:
                logger.debug("### continue transaction... expecting more flow files.");
                commSession.setStatus(Transaction.TransactionState.DATA_EXCHANGED);
                break;
            case BAD_CHECKSUM:
                logger.debug("received BAD_CHECKSUM.");
                commSession.setStatus(Transaction.TransactionState.ERROR);
                throw new IOException("Checksum didn't match. BAD_CHECKSUM.");
            case CONFIRM_TRANSACTION:
                logger.debug("### transaction is confirmed.");
                commSession.setStatus(Transaction.TransactionState.TRANSACTION_CONFIRMED);
                break;
            case FINISH_TRANSACTION:
                logger.debug("### transaction is ccompleted.");
                commSession.setStatus(Transaction.TransactionState.TRANSACTION_COMPLETED);
                break;
        }
    }

    @Override
    protected Response readTransactionResponse(CommunicationsSession commsSession) throws IOException {
        // Returns Response based on current status.
        HttpCommunicationsSession commSession = (HttpCommunicationsSession) commsSession;

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        switch (commSession.getStatus()){
            case DATA_EXCHANGED:
                String clientChecksum = ((HttpCommunicationsSession)commsSession).getChecksum();
                logger.info("### readTransactionResponse. clientChecksum=" + clientChecksum);
                ResponseCode.CONFIRM_TRANSACTION.writeResponse(new DataOutputStream(bos), clientChecksum);
                break;
            case TRANSACTION_CONFIRMED:
                logger.info("### readTransactionResponse. finishing.");
                ResponseCode.TRANSACTION_FINISHED.writeResponse(new DataOutputStream(bos));
                break;
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        return Response.read(new DataInputStream(bis));
    }

    @Override
    protected int commitTransferTransaction(Peer peer, FlowFileTransaction tx) throws IOException {
        // We don't commit the session here yet,
        // to avoid losing sent flow files in case some issue happens at client side while it is processing,
        // hold the transaction until we confirm additional request from client.
        HttpCommunicationsSession commSession = (HttpCommunicationsSession) peer.getCommunicationsSession();
        String txId = commSession.getTxId();
        logger.info("### Holding transaction. txId=" + txId);
        txOnHold.put(txId, tx);

        return tx.getFlowFilesSent().size();
    }

    public int commitTransferTransaction(Peer peer, String clientChecksum) throws IOException {
        logger.info("### Now, finally committing the transaction. clientChecksum=" + clientChecksum);
        HttpCommunicationsSession commSession = (HttpCommunicationsSession) peer.getCommunicationsSession();
        String txId = commSession.getTxId();
        FlowFileTransaction tx = txOnHold.get(txId);
        if(tx == null){
            throw new IOException("Transaction was not found. txId=" + txId);
        }
        commSession.setChecksum(clientChecksum);
        commSession.setStatus(Transaction.TransactionState.DATA_EXCHANGED);
        return super.commitTransferTransaction(peer, tx);
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

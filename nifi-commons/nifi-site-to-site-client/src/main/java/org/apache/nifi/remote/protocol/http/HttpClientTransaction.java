package org.apache.nifi.remote.protocol.http;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.remote.AbstractTransaction;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.http.SiteToSiteRestApiUtil;
import org.apache.nifi.remote.codec.StandardFlowFileCodec;
import org.apache.nifi.remote.io.http.HttpCommunicationsSession;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.socket.Response;
import org.apache.nifi.remote.protocol.socket.ResponseCode;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;

import javax.net.ssl.SSLContext;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class HttpClientTransaction extends AbstractTransaction {

    private SiteToSiteRestApiUtil apiUtil;
    private String holdUri;

    public HttpClientTransaction(final int protocolVersion, final Peer peer, TransferDirection direction, final boolean useCompression, final String portId, int penaltyMillis, EventReporter eventReporter) throws IOException {
        super(peer, direction, useCompression, new StandardFlowFileCodec(), eventReporter, protocolVersion, penaltyMillis, portId);
    }

    public void initialize(SiteToSiteRestApiUtil apiUtil) throws IOException {
        this.apiUtil = apiUtil;
        if(TransferDirection.SEND.equals(direction)){
            apiUtil.openConnectionForSend(destinationId, peer.getCommunicationsSession());
        } else {
            holdUri = apiUtil.openConnectionForReceive(destinationId, peer.getCommunicationsSession());
            dataAvailable = (holdUri != null);
        }
    }

    @Override
    protected void checkReceivedPacket(DataPacket packet) throws IOException {
        if(packet instanceof HttpControlPacket){
            BufferedReader br = new BufferedReader(new InputStreamReader(packet.getData()));
            throw new IOException(br.readLine());
        }
    }

    @Override
    protected Response readTransactionResponse() throws IOException {
        HttpCommunicationsSession commSession = (HttpCommunicationsSession) peer.getCommunicationsSession();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        if(TransferDirection.SEND.equals(direction)){
            switch (state){
                case DATA_EXCHANGED:
                    // Some flow files have been sent via stream, finish transferring.
                    holdUri = apiUtil.finishTransferFlowFiles(commSession);
                    ResponseCode.CONFIRM_TRANSACTION.writeResponse(new DataOutputStream(bos), commSession.getChecksum());
                    break;
                case TRANSACTION_CONFIRMED:
                    ResponseCode.TRANSACTION_FINISHED.writeResponse(new DataOutputStream(bos));
                    break;
            }
        } else {
            switch (state){
                case TRANSACTION_STARTED:
                case DATA_EXCHANGED:
                    if(StringUtils.isEmpty(commSession.getChecksum())){
                        logger.debug("readTransactionResponse. returning CONTINUE_TRANSACTION.");
                        // We don't know if there's more data to receive, so just continue it.
                        ResponseCode.CONTINUE_TRANSACTION.writeResponse(new DataOutputStream(bos));
                    } else {
                        // We got a checksum to send to server.
                        if(holdUri == null){
                            logger.debug("There's no transaction to confirm.");
                        } else {
                            // TODO: Write Confirmed or BadChecksum accordingly.
                            apiUtil.commitReceivingFlowFiles(holdUri, commSession.getChecksum());
                        }
                        ResponseCode.CONFIRM_TRANSACTION.writeResponse(new DataOutputStream(bos), "");
                    }
                    break;
            }
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        return Response.read(new DataInputStream(bis));
    }

    @Override
    protected void writeTransactionResponse(ResponseCode response, String explanation) throws IOException {
        HttpCommunicationsSession commSession = (HttpCommunicationsSession) peer.getCommunicationsSession();
        if(TransferDirection.SEND.equals(direction)){
            switch (response) {
                case FINISH_TRANSACTION:
                    logger.debug("{} Finishing transaction.", this);
                    break;
                case BAD_CHECKSUM:
                    // TODO: send cancel request to server.
                    apiUtil.cancelTransferFlowFiles(holdUri);
                    break;
                case CONFIRM_TRANSACTION:
                    apiUtil.commitTransferFlowFiles(holdUri);
                    break;
            }
        } else {
            switch (response) {
                 case CONFIRM_TRANSACTION:
                    logger.debug("{} Confirming transaction. checksum={}", this, explanation);
                    commSession.setChecksum(explanation);
                    break;
                case TRANSACTION_FINISHED:
                    logger.debug("{} Finishing transaction.", this);
                    break;
            }
        }
    }

}


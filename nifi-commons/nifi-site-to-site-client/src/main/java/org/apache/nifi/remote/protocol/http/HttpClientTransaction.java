package org.apache.nifi.remote.protocol.http;

import org.apache.nifi.remote.AbstractTransaction;
import org.apache.nifi.remote.ClientTransactionCompletion;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.TransactionCompletion;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.http.SiteToSiteRestApiUtil;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.codec.StandardFlowFileCodec;
import org.apache.nifi.remote.protocol.DataPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Set;

public class HttpClientTransaction extends AbstractTransaction {
    private static final Logger logger = LoggerFactory.getLogger(HttpClientTransaction.class);

    private final SiteToSiteRestApiUtil apiUtil;
    private final String portId;
    private Set<DataPacket> buffer;
    private Iterator<DataPacket> bufferIterator;
    private FlowFileCodec codec;

    public HttpClientTransaction(final Peer peer, TransferDirection direction, final String portId, final SSLContext sslContext, final int timeoutMillis) throws IOException {
        super(peer, direction);
        apiUtil = new SiteToSiteRestApiUtil(sslContext);
        apiUtil.setBaseUrl(peer.getUrl());
        apiUtil.setConnectTimeoutMillis(timeoutMillis);
        apiUtil.setReadTimeoutMillis(timeoutMillis);
        this.portId = portId;
        codec = new StandardFlowFileCodec();

        if(TransferDirection.SEND.equals(direction)){
            apiUtil.openConnectionForSend(portId, peer.getCommunicationsSession());
        } else {
            apiUtil.openConnectionForReceive(portId, peer.getCommunicationsSession());
        }
    }

    @Override
    public void send(DataPacket dataPacket) throws IOException {
        logger.info("### Buffering dataPacket to send to port: " + portId);
        OutputStream os = peer.getCommunicationsSession().getOutput().getOutputStream();
        codec.encode(dataPacket, os);
    }

    @Override
    public DataPacket receive() throws IOException {
        // TODO: This logic should be similar to SiteToSiteRestAPiUtil or Codec.
        InputStream is = peer.getCommunicationsSession().getInput().getInputStream();
        return codec.decode(is);
    }

    @Override
    public void confirm() throws IOException {
        if(TransferDirection.SEND.equals(direction)){
            // TODO: Flush the output stream
            apiUtil.transferFlowFile(portId, peer.getCommunicationsSession());
            // TODO: Get Response

            // TODO: Send confirmation to TX
        }
    }

    @Override
    public TransactionCompletion complete() throws IOException {
        // TODO: Create TransactionCompletion to return.
        if(TransferDirection.SEND.equals(direction)){
            return new ClientTransactionCompletion(false, 0, 0L, 0L);
        } else {

        }
        return new ClientTransactionCompletion(false, 0, 0L, 0L);
    }

    @Override
    public void cancel(String explanation) throws IOException {

    }

    @Override
    public void error() {

    }

}


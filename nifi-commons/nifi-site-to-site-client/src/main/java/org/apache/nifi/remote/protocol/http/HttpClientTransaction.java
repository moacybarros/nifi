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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Set;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

public class HttpClientTransaction extends AbstractTransaction {
    private static final Logger logger = LoggerFactory.getLogger(HttpClientTransaction.class);

    private final SiteToSiteRestApiUtil apiUtil;
    private final String portId;
    private Set<DataPacket> buffer;
    private Iterator<DataPacket> bufferIterator;
    private FlowFileCodec codec;
    private String holdUri;
    private final CRC32 crc = new CRC32();

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
            holdUri = apiUtil.openConnectionForReceive(portId, peer.getCommunicationsSession());
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
        InputStream is = peer.getCommunicationsSession().getInput().getInputStream();
        DataPacket packet = codec.decode(new CheckedInputStream(is, crc));
        if(packet instanceof HttpControlPacket){
            BufferedReader br = new BufferedReader(new InputStreamReader(packet.getData()));
            throw new IOException(br.readLine());
        }
        return packet;
    }

    @Override
    public void confirm() throws IOException {
        if(TransferDirection.SEND.equals(direction)){
            // TODO: Flush the output stream
            apiUtil.transferFlowFile(portId, peer.getCommunicationsSession());
            // TODO: Get Response

            // TODO: Send confirmation to TX
        } else {
            if(holdUri == null){
                logger.debug("There's no transaction to confirm.");
                return;
            }
            apiUtil.commitReceivingFlowFiles(holdUri, String.valueOf(crc.getValue()));
        }
    }

    @Override
    public TransactionCompletion complete() throws IOException {
        // TODO: Create TransactionCompletion to return.
        if(TransferDirection.SEND.equals(direction)){
            return new ClientTransactionCompletion(false, 0, 0L, 0L);
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


package org.apache.nifi.remote.protocol.http;

import org.apache.nifi.remote.AbstractTransaction;
import org.apache.nifi.remote.ClientTransactionCompletion;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.TransactionCompletion;
import org.apache.nifi.remote.protocol.DataPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HttpClientTransaction extends AbstractTransaction {
    private static final Logger logger = LoggerFactory.getLogger(HttpClientTransaction.class);

    public HttpClientTransaction(final Peer peer) throws IOException {
        super(peer);
    }

    @Override
    public void send(DataPacket dataPacket) throws IOException {
        // TODO: implementation.
    }

    @Override
    public DataPacket receive() throws IOException {
        // TODO: implementation.
        return null;
    }

    @Override
    public void confirm() throws IOException {
    }

    @Override
    public TransactionCompletion complete() throws IOException {
        return new ClientTransactionCompletion(false, 0, 0L, 0L);
    }

    @Override
    public void cancel(String explanation) throws IOException {

    }

    @Override
    public void error() {

    }

}


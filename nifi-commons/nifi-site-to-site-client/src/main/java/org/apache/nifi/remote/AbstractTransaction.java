package org.apache.nifi.remote;

import org.apache.nifi.remote.util.StandardDataPacket;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

public abstract class AbstractTransaction implements Transaction {
    protected final Peer peer;
    protected final TransferDirection direction;
    protected TransactionState state;

    public AbstractTransaction(final Peer peer, final TransferDirection direction) {
        this.peer = peer;
        this.state = TransactionState.TRANSACTION_STARTED;
        this.direction = direction;
    }

    @Override
    public void send(final byte[] content, final Map<String, String> attributes) throws IOException {
        send(new StandardDataPacket(attributes, new ByteArrayInputStream(content), content.length));
    }

    @Override
    public void error() {
        this.state = TransactionState.ERROR;
    }

    @Override
    public TransactionState getState() {
        return state;
    }

    @Override
    public Communicant getCommunicant() {
        return peer;
    }

}

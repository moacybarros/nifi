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
package org.apache.nifi.remote.io.http;

import org.apache.nifi.remote.AbstractCommunicationsSession;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.protocol.CommunicationsInput;
import org.apache.nifi.remote.protocol.CommunicationsOutput;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HttpCommunicationsSession extends AbstractCommunicationsSession {

    private int timeout = 30000;

    private final HttpInput input;
    private final HttpOutput output;
    private String transactionId;
    private String checksum;
    private Transaction.TransactionState status = Transaction.TransactionState.TRANSACTION_STARTED;

    public HttpCommunicationsSession(InputStream inputStream, OutputStream outputStream, String transactionId){
        this();
        input.setInputStream(inputStream);
        output.setOutputStream(outputStream);
        this.transactionId = transactionId;
    }

    public HttpCommunicationsSession(){
        super(null);
        this.input = new HttpInput();
        this.output = new HttpOutput();
    }

    @Override
    public void setTimeout(final int millis) throws IOException {
        this.timeout = millis;
    }

    @Override
    public int getTimeout() throws IOException {
        return timeout;
    }

    @Override
    public CommunicationsInput getInput() {
        return input;
    }

    @Override
    public CommunicationsOutput getOutput() {
        return output;
    }

    @Override
    public boolean isDataAvailable() {
        return false;
    }

    @Override
    public long getBytesWritten() {
        return 0;
    }

    @Override
    public long getBytesRead() {
        return 0;
    }

    @Override
    public void interrupt() {
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() throws IOException {

    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }


    public String getChecksum() {
        return checksum;
    }

    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }


    public Transaction.TransactionState getStatus() {
        return status;
    }

    public void setStatus(Transaction.TransactionState status) {
        this.status = status;
    }
}

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
package org.apache.nifi.remote.codec;

import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class StandardFlowFileCodecN extends StandardFlowFileCodec {

    private static final Logger logger = LoggerFactory.getLogger(StandardFlowFileCodecN.class);

    @Override
    public void encode(DataPacket dataPacket, OutputStream encodedOut) throws IOException {
        final CountDownLatch dataPacketIsSent = new CountDownLatch(1);
        final Thread sendingThread = new Thread(() -> {
            try {
                super.encode(dataPacket, encodedOut);
                dataPacketIsSent.countDown();
            } catch (Exception e) {
                logger.error("Failed to send packet.", e);
            }
        });
        sendingThread.start();

        try {
            if (!dataPacketIsSent.await(5, TimeUnit.SECONDS)) {
                throw new IOException(new TimeoutException("Sending dataPacket has been timeout."));
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}

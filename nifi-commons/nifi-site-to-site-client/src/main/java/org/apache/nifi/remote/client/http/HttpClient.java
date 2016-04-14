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
package org.apache.nifi.remote.client.http;

import org.apache.nifi.remote.*;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.client.socket.EndpointConnection;
import org.apache.nifi.remote.client.socket.EndpointConnectionPool;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.PortNotRunningException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.exception.UnknownPortException;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.util.ObjectHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HttpClient implements SiteToSiteClient {

    private static final Logger logger = LoggerFactory.getLogger(HttpClient.class);

    public HttpClient(final SiteToSiteClientConfig config) {
        logger.info("### creating new HttpClient, config=" + config, new RuntimeException());
    }

    @Override
    public Transaction createTransaction(TransferDirection direction) throws HandshakeException, PortNotRunningException, ProtocolException, UnknownPortException, IOException {
        return null;
    }

    @Override
    public boolean isSecure() throws IOException {
        return false;
    }

    @Override
    public SiteToSiteClientConfig getConfig() {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}

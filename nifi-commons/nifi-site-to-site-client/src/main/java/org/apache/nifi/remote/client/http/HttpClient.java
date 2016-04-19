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
import org.apache.nifi.remote.client.AbstractSiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.PortNotRunningException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.exception.UnknownPortException;
import org.apache.nifi.remote.io.http.HttpCommunicationsSession;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.http.HttpClientTransaction;
import org.apache.nifi.remote.util.NiFiRestApiUtil;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class HttpClient extends AbstractSiteToSiteClient {

    private static final Logger logger = LoggerFactory.getLogger(HttpClient.class);

    public HttpClient(final SiteToSiteClientConfig config) {
        super(config);
    }

    @Override
    public Transaction createTransaction(TransferDirection direction) throws HandshakeException, PortNotRunningException, ProtocolException, UnknownPortException, IOException {
        String apiUri = null;
        String clusterUrl = config.getUrl();
        try {
            apiUri = NiFiRestApiUtil.resolveApiUri(new URI(clusterUrl)) + "/site-to-site/peers";
        } catch (URISyntaxException e){
            throw new IllegalArgumentException("Invalid Cluster URL: " + clusterUrl);
        }
        logger.info("### sending API request to " + apiUri);
        NiFiRestApiUtil apiUtil = new NiFiRestApiUtil(config.getSslContext());
        Collection<PeerDTO> peers = apiUtil.getPeers(apiUri, (int) config.getTimeout(TimeUnit.MILLISECONDS));
        logger.info("### Got peers: " + peers);
        PeerDTO peerDTO = peers.iterator().next();

        PeerDescription description = new PeerDescription(peerDTO.getHostname(), peerDTO.getPort(), peerDTO.isSecure());
        CommunicationsSession commSession = new HttpCommunicationsSession(apiUri);
        Peer peer =  new Peer(description, commSession, apiUri, clusterUrl);

        HttpClientTransaction transaction = new HttpClientTransaction(peer);
        return transaction;
    }

    @Override
    public boolean isSecure() throws IOException {
        // TODO: check config.
        return false;
    }

    @Override
    public void close() throws IOException {
        // TODO: Do we have anything to clean up here? If we adopt connection pooling
    }
}

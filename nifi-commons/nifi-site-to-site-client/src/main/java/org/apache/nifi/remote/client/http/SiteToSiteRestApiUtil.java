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

import org.apache.nifi.remote.io.http.HttpInput;
import org.apache.nifi.remote.io.http.HttpOutput;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.util.NiFiRestApiUtil;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collection;

// TODO: I'd like to encapsulate this in HttpClientTransaction
public class SiteToSiteRestApiUtil extends NiFiRestApiUtil {

    private static final Logger logger = LoggerFactory.getLogger(SiteToSiteRestApiUtil.class);
    private HttpURLConnection urlConnection;

    public SiteToSiteRestApiUtil(SSLContext sslContext) {
        super(sslContext);
    }

    public Collection<PeerDTO> getPeers() throws IOException {
        return getEntity("/site-to-site/peers", PeersEntity.class).getPeers();
    }

    public void openConnectionForSend(String portId, CommunicationsSession commSession) throws IOException {
        logger.info("### openConnectionForSend to port: " + portId);

        urlConnection = getConnection("/site-to-site/ports/" + portId + "/flow-files");
        urlConnection.setDoOutput(true);
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("Content-Type", "application/flowfile-v3");
        urlConnection.setRequestProperty("Accept", "application/json");

        ((HttpOutput)commSession.getOutput()).setOutputStream(urlConnection.getOutputStream());

    }

    public void openConnectionForReceive(String portId, CommunicationsSession commSession) throws IOException {
        logger.info("### openConnectionForSend to port: " + portId);
        urlConnection = getConnection("/site-to-site/ports/" + portId + "/flow-files");
        urlConnection.setRequestMethod("GET");
        urlConnection.setRequestProperty("Accept", "application/octet-stream");

        ((HttpInput)commSession.getInput()).setInputStream(urlConnection.getInputStream());

        // TODO: Capture responseCode and transaction confirmation URL.
        int responseCode = urlConnection.getResponseCode();
        if(responseCode == 204){
            return;
        } else if(responseCode != 200){
            // TODO: more sophisticated error handling.
            throw new RuntimeException("Unexpected response code: " + responseCode);
        }

    }

    public void transferFlowFile(String portId, CommunicationsSession commSession) throws IOException {
        logger.info("### Sending transferFlowFile request to port: " + portId);

        commSession.getOutput().getOutputStream().flush();

        // TODO: Check response.
        ((HttpInput)commSession.getInput()).setInputStream(urlConnection.getInputStream());
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        StreamUtils.copy(commSession.getInput().getInputStream(), bos);
        String result = bos.toString("UTF-8");
        logger.info("### Sent request to port: " + portId + " result=" + result);
    }

}

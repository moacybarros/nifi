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
package org.apache.nifi.remote.client.socket;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.protocol.CommunicationsInput;
import org.apache.nifi.remote.protocol.CommunicationsOutput;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.remote.protocol.http.HttpProxy;
import org.apache.nifi.remote.util.SiteToSiteRestApiClient;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestSiteToSiteClient {

    private static final Logger logger = LoggerFactory.getLogger(TestSiteToSiteClient.class);

    @Test
    @Ignore("For local testing only; not really a unit test but a manual test")
    public void testReceiveLocal() throws Exception {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.remote", "TRACE");
        System.setProperty("org.apache.nifi.remote.client.socket.TestSiteToSiteClient", "TRACE");

        SSLContext sslContext = getSslContext();
        final SiteToSiteClient client = new SiteToSiteClient.Builder()
//                .url("http://0.c.nifi.aws.mine:8080/nifi")
                .url("http://localhost:8084/nifi")
                .portName("output-1")
                .requestBatchCount(10)
                .transportProtocol(SiteToSiteTransportProtocol.HTTP)
//                .proxyHost("0.proxy.aws.mine")
//                .proxyPort(8080)
                .sslContext(sslContext)
                .build();

        try {
            for (int i = 0; i < 1; i++) {
                final Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);
                Assert.assertNotNull(transaction);

                DataPacket packet;
                while (true) {
                    packet = transaction.receive();
                    if (packet == null) {
                        break;
                    }

                    final InputStream in = packet.getData();
                    final long size = packet.getSize();
                    final byte[] buff = new byte[(int) size];

                    StreamUtils.fillBuffer(in, buff);
                    Thread.sleep(0_000);
                }

                CommunicationsInput input = ((Peer) transaction.getCommunicant()).getCommunicationsSession().getInput();
                logger.info("bytesRead={}", input.getBytesRead());

                transaction.confirm();
//               transaction.cancel("Cancel test");
                transaction.complete();
            }
        } finally {
            client.close();
        }
    }


    @Test
    @Ignore("For local testing only; not really a unit test but a manual test")
    public void testReceive() throws Exception {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.remote", "DEBUG");

        // Traffic Server
//        HttpProxy proxy = new HttpProxy("0.proxy.aws.mine", 8080, null, null);
        // Squid
        HttpProxy proxy = new HttpProxy("0.proxy.aws.mine", 3128, "nifi", "nifi proxy password");

        SSLContext sslContext = getSslContext();
        final SiteToSiteClient client = new SiteToSiteClient.Builder()
//                .url("http://0.c.nifi.aws.mine:8080/nifi")
                .url("https://0.b.nifi.aws.mine:8443/nifi")
                .portName("uptime")
                .requestBatchCount(10)
                .transportProtocol(SiteToSiteTransportProtocol.HTTP)
//                .proxyHost("0.apache.aws.mine")
//                .proxyPort(80)
                .httpProxy(proxy)
                 .sslContext(sslContext)
                .build();

        try {
            for (int i = 0; i < 1; i++) {
                final Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);
                Assert.assertNotNull(transaction);

                DataPacket packet;
                while (true) {
                    packet = transaction.receive();
                    if (packet == null) {
                        break;
                    }

                    final InputStream in = packet.getData();
                    final long size = packet.getSize();
                    final byte[] buff = new byte[(int) size];

                    StreamUtils.fillBuffer(in, buff);
                    Thread.sleep(0_000);

                }

//                transaction.cancel("SSL Cancel!");
                transaction.confirm();
                transaction.complete();
            }
        } finally {
            client.close();
        }
    }

    private SSLContext getSslContext() throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException, UnrecoverableKeyException, KeyManagementException {
        String keyStore = "/Users/koji/dev/nifi-local-http-s2s-demo/local-demo-keystore.jks";
        String keystorePasswd = "keystorepass";
        String keystoreType = "JKS";
        String truststore = "/Users/koji/dev/nifi-local-http-s2s-demo/local-demo-truststore.jks";
        String truststorePasswd = "truststorepass";
        String truststoreType = "JKS";
        String protocol = "TLS";
        return SslContextFactory.createSslContext(keyStore, keystorePasswd.toCharArray(), keystoreType,
                truststore, truststorePasswd.toCharArray(), truststoreType, SslContextFactory.ClientAuth.NONE, protocol);
    }

    @Test
    @Ignore("For local testing only; not really a unit test but a manual test")
    public void testSend() throws Exception {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.remote", "DEBUG");
        System.setProperty("org.apache.nifi.remote.client.socket.TestSiteToSiteClient", "DEBUG");

//        String clusterUrl = "http://localhost:8084/nifi";
//        String clusterUrl = "http://localhost:8080/nifi";
        String clusterUrl = "https://0.b.nifi.aws.mine:8443/nifi";
//        String clusterUrl = "http://0.a.nifi.aws.mine:8080/nifi";
        String portName = "input-1";

        String proxyHost = "0.proxy.aws.mine";
//        int proxyPort = 3128;
//        String proxyUser = "nifi";
//        String proxyPassword = "nifi proxy password";
        int proxyPort = 8080;
        String proxyUser = null;
        String proxyPassword = null;

//        HttpProxy proxy = new HttpProxy(proxyHost, proxyPort, proxyUser, proxyPassword);
        HttpProxy proxy = null;

        final int numOfThread = 1;
        final int numOfFilesPerThread = 3;

        ExecutorService threadPool = Executors.newFixedThreadPool(numOfThread);

        for (int i = 0; i < numOfThread; i++) {
            final int fi = i;
            threadPool.submit(() -> {
                try (
                    final SiteToSiteClient client = new SiteToSiteClient.Builder()
                            .url(clusterUrl)
                            .portName(portName)
                            .transportProtocol(SiteToSiteTransportProtocol.HTTP)
                            .sslContext(getSslContext())
                            .httpProxy(proxy)
                            .build();
                ) {

                    final Transaction transaction = client.createTransaction(TransferDirection.SEND);
                    Assert.assertNotNull(transaction);

                    for (int j = 0; j < numOfFilesPerThread; j++) {
                        final Map<String, String> attrs = new HashMap<>();
                        attrs.put("site-to-site", "yes, please! " + fi + ":" + j);
                        final byte[] bytes = "Hello".getBytes();
                        final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                        final DataPacket packet = new StandardDataPacket(attrs, bais, bytes.length);
                        transaction.send(packet);

//            Thread.sleep(1_000);
                    }

                    CommunicationsOutput output = ((Peer) transaction.getCommunicant()).getCommunicationsSession().getOutput();
                    logger.info("bytesWritten={}", output.getBytesWritten());

                    transaction.confirm();
//            transaction.cancel("Cancel test");
                    transaction.complete();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.HOURS);
    }

    @Test
    public void testSerialization() {
        final SiteToSiteClientConfig clientConfig = new SiteToSiteClient.Builder()
                .url("http://localhost:8080/nifi")
                .portName("input")
                .buildConfig();

        final Kryo kryo = new Kryo();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final Output output = new Output(out);

        try {
            kryo.writeObject(output, clientConfig);
        } finally {
            output.close();
        }

        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        final Input input = new Input(in);

        try {
            SiteToSiteClientConfig clientConfig2 = kryo.readObject(input, SiteToSiteClient.StandardSiteToSiteClientConfig.class);
            Assert.assertEquals(clientConfig.getUrl(), clientConfig2.getUrl());
        } finally {
            input.close();
        }
    }

    @Test
    public void testController() throws Exception {
        SSLContext sslContext = getSslContext();

        String proxyHost = "0.proxy.aws.mine";
        int proxyPort = 3128;
        final HttpProxy proxy = new HttpProxy(proxyHost, proxyPort, "nifi", "nifi proxy password");

        try (SiteToSiteRestApiClient api = new SiteToSiteRestApiClient(sslContext, proxy)) {
            api.setBaseUrl("https://0.b.nifi.aws.mine:8443/nifi-api");
            ControllerDTO controller = api.getController();

            Collection<PeerDTO> peers = api.getPeers();

            api.initiateTransaction(TransferDirection.SEND, "0f6ddadb-f07d-4be2-b324-bf41a3edae6e");
        }
    }

}

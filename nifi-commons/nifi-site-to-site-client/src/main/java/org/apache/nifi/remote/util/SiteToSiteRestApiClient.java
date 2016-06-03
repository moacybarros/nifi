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
package org.apache.nifi.remote.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.entity.ContentProducer;
import org.apache.http.entity.EntityTemplate;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.util.EntityUtils;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.http.TransportProtocolVersionNegotiator;
import org.apache.nifi.remote.exception.PortNotRunningException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.exception.UnknownPortException;
import org.apache.nifi.remote.io.http.HttpCommunicationsSession;
import org.apache.nifi.remote.io.http.HttpInput;
import org.apache.nifi.remote.io.http.HttpOutput;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.remote.protocol.http.HttpHeaders;
import org.apache.nifi.remote.protocol.http.HttpProxy;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_COUNT;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_DURATION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_SIZE;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_REQUEST_EXPIRATION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_USE_COMPRESSION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_HEADER_NAME;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_URI_INTENT_NAME;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_URI_INTENT_VALUE;

public class SiteToSiteRestApiClient {

    protected static final int RESPONSE_CODE_OK = 200;
    protected static final int RESPONSE_CODE_CREATED = 201;
    protected static final int RESPONSE_CODE_ACCEPTED = 202;
    protected static final int RESPONSE_CODE_SEE_OTHER = 303;
    protected static final int RESPONSE_CODE_BAD_REQUEST = 400;
    protected static final int RESPONSE_CODE_UNAUTHORIZED = 401;
    protected static final int RESPONSE_CODE_NOT_FOUND = 404;
    protected static final int RESPONSE_CODE_SERVICE_UNAVAILABLE = 503;

    private static final Logger logger = LoggerFactory.getLogger(SiteToSiteRestApiClient.class);

    private String baseUrl;
    protected final SSLContext sslContext;
    protected final HttpProxy proxy;
    private RequestConfig requestConfig;

    private boolean compress = false;
    private int requestExpirationMillis = 0;
    private int serverTransactionTtl = 0;
    private int batchCount = 0;
    private long batchSize = 0;
    private long batchDurationMillis = 0;
    private TransportProtocolVersionNegotiator transportProtocolVersionNegotiator = new TransportProtocolVersionNegotiator(1);

    private String trustedPeerDn;
    private final ScheduledExecutorService ttlExtendTaskExecutor;
    private ScheduledFuture<?> ttlExtendingThread;

    protected int connectTimeoutMillis;
    protected int readTimeoutMillis;
    private static final Pattern HTTP_ABS_URL = Pattern.compile("^https?://.+$");


    private CloseableHttpClient httpClient;

    public SiteToSiteRestApiClient(final SSLContext sslContext, final HttpProxy proxy) {
        this.sslContext = sslContext;
        this.proxy = proxy;
        ttlExtendTaskExecutor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(final Runnable r) {
                final Thread thread = defaultFactory.newThread(r);
                thread.setName(Thread.currentThread().getName() + " TTLExtend");
                return thread;
            }
        });
    }

    protected CloseableHttpClient getHttpClient() {
        if (httpClient == null) {
            setupClient();
        }
        return httpClient;
    }

    protected RequestConfig getRequestConfig() {
        if (requestConfig == null) {
            setupClient();
        }
        return requestConfig;
    }

    protected void setupClient() {
        final RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
                .setConnectionRequestTimeout(connectTimeoutMillis)
                .setConnectTimeout(connectTimeoutMillis)
                .setSocketTimeout(readTimeoutMillis);

        HttpClientBuilder clientBuilder = HttpClients.custom();
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        if (proxy != null) {
            HttpHost proxyHost = proxy.getHttpHost();
            requestConfigBuilder.setProxy(proxyHost);

            if (!isEmpty(proxy.getUsername()) && !isEmpty(proxy.getPassword())) {
                credsProvider.setCredentials(
                        new AuthScope(proxyHost),
                        new UsernamePasswordCredentials(proxy.getUsername(), proxy.getPassword()));
            }

        }

        if (sslContext != null) {
            clientBuilder.setSslcontext(sslContext);
            clientBuilder.addInterceptorFirst(new HttpResponseInterceptor() {
                @Override
                public void process(final HttpResponse response, final HttpContext httpContext) throws HttpException, IOException {
                    final HttpCoreContext coreContext = HttpCoreContext.adapt(httpContext);
                    final ManagedHttpClientConnection conn = coreContext.getConnection(ManagedHttpClientConnection.class);
                    if (!conn.isOpen()) {
                        return;
                    }

                    final SSLSession sslSession = conn.getSSLSession();

                    if (sslSession != null) {
                        final Certificate[] certChain = sslSession.getPeerCertificates();
                        if (certChain == null || certChain.length == 0) {
                            throw new SSLPeerUnverifiedException("No certificates found");
                        }

                        try {
                            final X509Certificate cert = CertificateUtils.convertAbstractX509Certificate(certChain[0]);
                            trustedPeerDn = cert.getSubjectDN().getName().trim();
                        } catch (CertificateException e) {
                            final String msg = "Could not extract subject DN from SSL session peer certificate";
                            logger.warn(msg);
                            throw new SSLPeerUnverifiedException(msg);
                        }
                    }
                }
            });
        }

        requestConfig = requestConfigBuilder.build();
        httpClient = clientBuilder
                .setDefaultCredentialsProvider(credsProvider).build();
    }

    public ControllerDTO getController() throws IOException {
        try {
            HttpGet get = createGet("/site-to-site");
            get.setHeader(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));
            return execute(get, ControllerEntity.class).getController();

        } catch (HttpGetFailedException e) {
            if (RESPONSE_CODE_NOT_FOUND == e.getResponseCode()) {
                logger.debug("getController received NOT_FOUND, trying to access the old NiFi version resource url...");
                HttpGet get = createGet("/controller");
                return execute(get, ControllerEntity.class).getController();
            }
            throw e;
        }
    }

    public Collection<PeerDTO> getPeers() throws IOException {
        HttpGet get = createGet("/site-to-site/peers");
        get.setHeader(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));
        return execute(get, PeersEntity.class).getPeers();
    }

    public String initiateTransaction(TransferDirection direction, String portId) throws IOException {
        if (TransferDirection.RECEIVE.equals(direction)) {
            return initiateTransaction("output-ports", portId);
        } else {
            return initiateTransaction("input-ports", portId);
        }
    }

    private String initiateTransaction(String portType, String portId) throws IOException {
        logger.debug("initiateTransaction handshaking portType={}, portId={}", portType, portId);
        HttpPost post = createPost("/site-to-site/" + portType + "/" + portId + "/transactions");


        post.setHeader("Accept", "application/json");
        post.setHeader(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));

        setHandshakeProperties(post);

        try (CloseableHttpResponse response = getHttpClient().execute(post)) {
            int responseCode = response.getStatusLine().getStatusCode();
            logger.debug("initiateTransaction responseCode={}", responseCode);

            String transactionUrl;
            switch (responseCode) {
                case RESPONSE_CODE_CREATED :
                    EntityUtils.consume(response.getEntity());

                    transactionUrl = readTransactionUrl(response);
                    if (isEmpty(transactionUrl)) {
                        throw new ProtocolException("Server returned RESPONSE_CODE_CREATED without Location header");
                    }
                    Header transportProtocolVersionHeader = response.getFirstHeader(HttpHeaders.PROTOCOL_VERSION);
                    if (transportProtocolVersionHeader == null) {
                        throw new ProtocolException("Server didn't return confirmed protocol version");
                    }
                    Integer protocolVersionConfirmedByServer = Integer.valueOf(transportProtocolVersionHeader.getValue());
                    logger.debug("Finished version negotiation, protocolVersionConfirmedByServer={}", protocolVersionConfirmedByServer);
                    transportProtocolVersionNegotiator.setVersion(protocolVersionConfirmedByServer);

                    Header serverTransactionTtlHeader = response.getFirstHeader(HttpHeaders.SERVER_SIDE_TRANSACTION_TTL);
                    if (serverTransactionTtlHeader == null) {
                        throw new ProtocolException("Server didn't return " + HttpHeaders.SERVER_SIDE_TRANSACTION_TTL);
                    }
                    serverTransactionTtl = Integer.parseInt(serverTransactionTtlHeader.getValue());
                    break;

                default:
                    try (InputStream content = response.getEntity().getContent()) {
                        throw handleErrResponse(responseCode, content);
                    }
            }
            logger.debug("initiateTransaction handshaking finished, transactionUrl={}", transactionUrl);
            return transactionUrl;
        }

    }

    public boolean openConnectionForReceive(String transactionUrl, CommunicationsSession commSession) throws IOException {

        HttpGet get = createGet(transactionUrl + "/flow-files");
        get.setHeader(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));

        setHandshakeProperties(get);

        CloseableHttpResponse response = getHttpClient().execute(get);
        int responseCode = response.getStatusLine().getStatusCode();
        logger.debug("responseCode={}", responseCode);

        boolean keepItOpen = false;
        try {
            switch (responseCode) {
                case RESPONSE_CODE_OK :
                    logger.debug("Server returned RESPONSE_CODE_OK, indicating there was no data.");
                    EntityUtils.consume(response.getEntity());
                    return false;

                case RESPONSE_CODE_ACCEPTED :
                    InputStream httpIn = response.getEntity().getContent();
                    InputStream streamCapture = new InputStream() {
                        boolean closed = false;
                        @Override
                        public int read() throws IOException {
                            if(closed) return -1;
                            int r = httpIn.read();
                            if (r < 0) {
                                closed = true;
                                logger.debug("Reached to end of input stream. Closing resources...");
                                stopExtendingTtl();
                                closeSilently(httpIn);
                                closeSilently(response);
                            }
                            return r;
                        }
                    };
                    ((HttpInput)commSession.getInput()).setInputStream(streamCapture);

                    startExtendingTtl(transactionUrl, httpIn, response);
                    keepItOpen = true;
                    return true;

                default:
                    try (InputStream content = response.getEntity().getContent()) {
                        throw handleErrResponse(responseCode, content);
                    }
            }
        } finally {
            if (!keepItOpen) {
                response.close();
            }
        }
    }

    private Future<String> postRequestForSend;
    private CountDownLatch finishTransferFlowFiles;
    public void openConnectionForSend(String transactionUrl, CommunicationsSession commSession) throws IOException {

        HttpPost post = createPost(transactionUrl + "/flow-files");

        post.setHeader("Content-Type", "application/octet-stream");
        post.setHeader("Accept", "text/plain");
        post.setHeader(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));

        setHandshakeProperties(post);

        ExecutorService executorService = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(final Runnable r) {
                final Thread thread = defaultFactory.newThread(r);
                thread.setName(Thread.currentThread().getName() + " Http Sender");
                return thread;
            }
        });

        CountDownLatch initConnectionLatch = new CountDownLatch(1);
        finishTransferFlowFiles = new CountDownLatch(1);

        postRequestForSend = executorService.submit(() -> {
            final EntityTemplate entity = new EntityTemplate(new ContentProducer() {
                @Override
                public void writeTo(final OutputStream httpOut) throws IOException {
                    final AtomicLong lastDataSentAt = new AtomicLong(0);

                    OutputStream streamCapture = new OutputStream(){
                        @Override
                        public void write(int b) throws IOException {
                            httpOut.write(b);
                            startExtendingTtl(transactionUrl, httpOut, null);
                            lastDataSentAt.set(System.currentTimeMillis());
                        }

                    };

                    // Pass the output stream so that Site-to-Site client thread can send
                    // data packet through this connection.
                    logger.debug("writeTo {} has started...", transactionUrl);
                    ((HttpOutput)commSession.getOutput()).setOutputStream(streamCapture);
                    initConnectionLatch.countDown();

                    try {
                        while (true) {
                            logger.debug("Waiting for finishTransferFlowFiles to be called for {} sec...", serverTransactionTtl);
                            if (!finishTransferFlowFiles.await(serverTransactionTtl, TimeUnit.SECONDS)) {
                                long elapsedSinceLastDataSent = System.currentTimeMillis() - lastDataSentAt.get();
                                if (elapsedSinceLastDataSent > TimeUnit.MILLISECONDS.convert(serverTransactionTtl, TimeUnit.SECONDS)) {
                                    throw new IOException("Awaiting finishTransferFlowFiles has been timeout.");
                                }
                            }
                            return;
                        }
                    } catch (InterruptedException e) {
                        throw new IOException("Awaiting finishTransferFlowFiles has been interrupted.", e);
                    }

                }
            }) {
                @Override
                public boolean isStreaming() {
                    return true;
                }
            };


            post.setEntity(entity);

            try (CloseableHttpResponse response = getHttpClient().execute(post)) {

                int responseCode = response.getStatusLine().getStatusCode();

                switch (responseCode) {
                    case RESPONSE_CODE_ACCEPTED :
                        String receivedChecksum = EntityUtils.toString(response.getEntity());
                        ((HttpInput)commSession.getInput()).setInputStream(new ByteArrayInputStream(receivedChecksum.getBytes()));
                        ((HttpCommunicationsSession)commSession).setChecksum(receivedChecksum);
                        logger.debug("receivedChecksum={}", receivedChecksum);
                        return receivedChecksum;

                    default:
                        try (InputStream content = response.getEntity().getContent()) {
                            throw handleErrResponse(responseCode, content);
                        }
                }
            }

        });

        try {
            if (!initConnectionLatch.await(connectTimeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new IOException("Awaiting initConnectionLatch has been timeout.");
            }
        } catch (InterruptedException e) {
            throw new IOException("Awaiting initConnectionLatch has been interrupted.", e);
        }

        executorService.shutdown();
    }

    public void finishTransferFlowFiles(CommunicationsSession commSession) throws IOException {

        commSession.getOutput().getOutputStream().flush();

        stopExtendingTtl();

        if (postRequestForSend == null) {
            new IllegalStateException("Data transfer has not started yet.");
        }

        try {
            finishTransferFlowFiles.countDown();
            postRequestForSend.get(readTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            logger.debug("Something has happened at sending thread. {}", e.getMessage());
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            } else {
                throw new IOException(cause);
            }
        } catch (TimeoutException|InterruptedException e) {
            throw new IOException(e);
        }

    }




    private void startExtendingTtl(final String transactionUrl, final Closeable stream, final CloseableHttpResponse response) {
        if (ttlExtendingThread != null) {
            // Already started.
            return;
        }
        logger.debug("Starting extending TTL thread...");
        final SiteToSiteRestApiClient extendingApi = new SiteToSiteRestApiClient(sslContext, proxy);
        extendingApi.transportProtocolVersionNegotiator = this.transportProtocolVersionNegotiator;
        extendingApi.connectTimeoutMillis = this.connectTimeoutMillis;
        extendingApi.readTimeoutMillis = this.readTimeoutMillis;
        int extendFrequency = serverTransactionTtl / 2;
        ttlExtendingThread = ttlExtendTaskExecutor.scheduleWithFixedDelay(() -> {
            try {
                extendingApi.extendTransaction(transactionUrl);
            } catch (Exception e) {
                logger.warn("Got an exception while extending transaction ttl", e);
                try {
                    stopExtendingTtl();
                } finally {
                    // Without disconnecting, Site-to-Site client keep reading data packet,
                    // while server has already rollback.
                    try {
                        closeSilently(stream);
                    } catch (Exception es) {
                        logger.warn("Got an exception while closing stream", es);
                    }
                    try {
                        closeSilently(response);
                    } catch (Exception er) {
                        logger.warn("Got an exception while closing response", er);
                    }
                }
            }
        }, extendFrequency, extendFrequency, TimeUnit.SECONDS);
    }

    private void closeSilently(final Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
            logger.warn("Got an exception during closing {}: {}", closeable, e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.warn("", e);
            }
        }
    }

    public TransactionResultEntity extendTransaction(String transactionUrl) throws IOException {
        logger.debug("Sending extendTransaction request to transactionUrl: {}", transactionUrl);

        final HttpPut put = createPut(transactionUrl);

        put.setHeader("Accept", "application/json");
        put.setHeader(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));

        setHandshakeProperties(put);

        try (CloseableHttpResponse response = getHttpClient().execute(put)) {
            int responseCode = response.getStatusLine().getStatusCode();
            logger.debug("extendTransaction responseCode={}", responseCode);

            try (InputStream content = response.getEntity().getContent()) {
                switch (responseCode) {
                    case RESPONSE_CODE_OK :
                        return readResponse(content);

                    default:
                        throw handleErrResponse(responseCode, content);
                }
            }
        }

    }

    private void stopExtendingTtl() {
        if (ttlExtendingThread != null && !ttlExtendingThread.isCancelled()) {
            logger.debug("Cancelling extending ttl...");
            ttlExtendingThread.cancel(true);
        }

        if (!ttlExtendTaskExecutor.isShutdown()) {
            ttlExtendTaskExecutor.shutdown();
        }
    }

    private IOException handleErrResponse(final int responseCode, final InputStream in) throws IOException {
        if(in == null) {
            return new IOException("Unexpected response code: " + responseCode);
        }
        TransactionResultEntity errEntity = readResponse(in);
        ResponseCode errCode = ResponseCode.fromCode(errEntity.getResponseCode());
        switch (errCode) {
            case UNKNOWN_PORT:
                return new UnknownPortException(errEntity.getMessage());
            case PORT_NOT_IN_VALID_STATE:
                return new PortNotRunningException(errEntity.getMessage());
            default:
                return new IOException("Unexpected response code: " + responseCode
                        + " errCode:" + errCode + " errMessage:" + errEntity.getMessage());
        }
    }

    private TransactionResultEntity readResponse(InputStream inputStream) throws IOException {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        StreamUtils.copy(inputStream, bos);
        String responseMessage = null;
        try {
            responseMessage = new String(bos.toByteArray(), "UTF-8");
            logger.debug("readResponse responseMessage={}", responseMessage);

            final ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(responseMessage, TransactionResultEntity.class);

        } catch (JsonParseException | JsonMappingException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to parse JSON.", e);
            }
            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.ABORT.getCode());
            entity.setMessage(responseMessage);
            return entity;
        }
    }

    private String readTransactionUrl(final CloseableHttpResponse response) {
        final Header locationUriIntentHeader = response.getFirstHeader(LOCATION_URI_INTENT_NAME);
        logger.debug("locationUriIntentHeader={}", locationUriIntentHeader);
        if (locationUriIntentHeader != null) {
            if (LOCATION_URI_INTENT_VALUE.equals(locationUriIntentHeader.getValue())) {
                Header transactionUrl = response.getFirstHeader(LOCATION_HEADER_NAME);
                logger.debug("transactionUrl={}", transactionUrl);
                if (transactionUrl != null) {
                    return transactionUrl.getValue();
                }
            }
        }
        return null;
    }

    private void setHandshakeProperties(final HttpRequestBase httpRequest) {
        if(compress) httpRequest.setHeader(HANDSHAKE_PROPERTY_USE_COMPRESSION, "true");
        if(requestExpirationMillis > 0) httpRequest.setHeader(HANDSHAKE_PROPERTY_REQUEST_EXPIRATION, String.valueOf(requestExpirationMillis));
        if(batchCount > 0) httpRequest.setHeader(HANDSHAKE_PROPERTY_BATCH_COUNT, String.valueOf(batchCount));
        if(batchSize > 0) httpRequest.setHeader(HANDSHAKE_PROPERTY_BATCH_SIZE, String.valueOf(batchSize));
        if(batchDurationMillis > 0) httpRequest.setHeader(HANDSHAKE_PROPERTY_BATCH_DURATION, String.valueOf(batchDurationMillis));
    }

    private HttpGet createGet(final String path) {
        final URI url = getUri(path);
        HttpGet get = new HttpGet(url);
        get.setConfig(getRequestConfig());
        return get;
    }

    private URI getUri(String path) {
        final URI url;
        try {
            if(HTTP_ABS_URL.matcher(path).find()){
                url = new URI(path);
            } else {
                if(StringUtils.isEmpty(getBaseUrl())){
                    throw new IllegalStateException("API baseUrl is not resolved yet, call setBaseUrl or resolveBaseUrl before sending requests with relative path.");
                }
                url = new URI(baseUrl + path);
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
        return url;
    }

    private HttpPost createPost(final String path) {
        final URI url = getUri(path);
        HttpPost post = new HttpPost(url);
        post.setConfig(getRequestConfig());
        return post;
    }

    private HttpPut createPut(final String path) {
        final URI url = getUri(path);
        HttpPut put = new HttpPut(url);
        put.setConfig(getRequestConfig());
        return put;
    }

    private HttpDelete createDelete(final String path) {
        final URI url = getUri(path);
        HttpDelete delete = new HttpDelete(url);
        delete.setConfig(getRequestConfig());
        return delete;
    }

    private String execute(final HttpGet get) throws IOException {

        CloseableHttpClient httpClient = getHttpClient();
        try (CloseableHttpResponse response = httpClient.execute(get)) {
            StatusLine statusLine = response.getStatusLine();
            int statusCode = statusLine.getStatusCode();
            if (RESPONSE_CODE_OK != statusCode) {
                throw new HttpGetFailedException(statusCode, statusLine.getReasonPhrase(), null);
            }
            HttpEntity entity = response.getEntity();
            String responseMessage = EntityUtils.toString(entity);
            return responseMessage;
        }
    }

    public class HttpGetFailedException extends IOException {
        private final int responseCode;
        private final String responseMessage;
        private final String explanation;
        public HttpGetFailedException(final int responseCode, final String responseMessage, final String explanation) {
            super("response code " + responseCode + ":" + responseMessage + " with explanation: " + explanation);
            this.responseCode = responseCode;
            this.responseMessage = responseMessage;
            this.explanation = explanation;
        }

        public int getResponseCode() {
            return responseCode;
        }

        public String getDescription() {
            return !isEmpty(explanation) ? explanation : responseMessage;
        }
    }


    private <T> T execute(final HttpGet get, final Class<T> entityClass) throws IOException {
        get.setHeader("Accept", "application/json");
        final String responseMessage = execute(get);

        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper.readValue(responseMessage, entityClass);
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(final String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public void setReadTimeoutMillis(int readTimeoutMillis) {
        this.readTimeoutMillis = readTimeoutMillis;
    }

    public String resolveBaseUrl(String clusterUrl) {
        URI clusterUri;
        try {
            clusterUri = new URI(clusterUrl);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Specified clusterUrl was: " + clusterUrl, e);
        }
        return this.resolveBaseUrl(clusterUri);
    }

    public String resolveBaseUrl(URI clusterUrl) {
        String urlPath = clusterUrl.getPath();
        if (urlPath.endsWith("/")) {
            urlPath = urlPath.substring(0, urlPath.length() - 1);
        }
        return resolveBaseUrl(clusterUrl.getScheme(), clusterUrl.getHost(), clusterUrl.getPort(), urlPath + "-api");
    }

    public String resolveBaseUrl(final String scheme, final String host, final int port) {
        return resolveBaseUrl(scheme, host, port, "/nifi-api");
    }

    public String resolveBaseUrl(final String scheme, final String host, final int port, String path) {
        String baseUri = scheme + "://" + host + ":" + port + path;
        this.setBaseUrl(baseUri);
        return baseUri;
    }

    public void setCompress(boolean compress) {
        this.compress = compress;
    }

    public void setRequestExpirationMillis(int requestExpirationMillis) {
        if(requestExpirationMillis < 0) throw new IllegalArgumentException("requestExpirationMillis can't be a negative value.");
        this.requestExpirationMillis = requestExpirationMillis;
    }

    public void setBatchCount(int batchCount) {
        if(batchCount < 0) throw new IllegalArgumentException("batchCount can't be a negative value.");
        this.batchCount = batchCount;
    }

    public void setBatchSize(long batchSize) {
        if(batchSize < 0) throw new IllegalArgumentException("batchSize can't be a negative value.");
        this.batchSize = batchSize;
    }

    public void setBatchDurationMillis(long batchDurationMillis) {
        if(batchDurationMillis < 0) throw new IllegalArgumentException("batchDurationMillis can't be a negative value.");
        this.batchDurationMillis = batchDurationMillis;
    }

    public Integer getTransactionProtocolVersion() {
        return transportProtocolVersionNegotiator.getTransactionProtocolVersion();
    }

    public String getTrustedPeerDn() {
        return this.trustedPeerDn;
    }

    public TransactionResultEntity commitReceivingFlowFiles(String transactionUrl, ResponseCode clientResponse, String checksum) throws IOException {
        logger.debug("Sending commitReceivingFlowFiles request to transactionUrl: {}, clientResponse={}, checksum={}",
                transactionUrl, clientResponse, checksum);

        stopExtendingTtl();

        StringBuilder urlBuilder = new StringBuilder(transactionUrl).append("?responseCode=").append(clientResponse.getCode());
        if (ResponseCode.CONFIRM_TRANSACTION.equals(clientResponse)) {
            urlBuilder.append("&checksum=").append(checksum);
        }

        HttpDelete delete = createDelete(urlBuilder.toString());
        delete.setHeader("Accept", "application/json");
        delete.setHeader(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));

        setHandshakeProperties(delete);

        try (CloseableHttpResponse response = getHttpClient().execute(delete)) {
            int responseCode = response.getStatusLine().getStatusCode();
            logger.debug("commitReceivingFlowFiles responseCode={}", responseCode);

            try (InputStream content = response.getEntity().getContent()) {
                switch (responseCode) {
                    case RESPONSE_CODE_OK :
                        return readResponse(content);

                    case RESPONSE_CODE_BAD_REQUEST :
                        return readResponse(content);

                    default:
                        throw handleErrResponse(responseCode, content);
                }
            }
        }

    }

    public TransactionResultEntity commitTransferFlowFiles(String transactionUrl, ResponseCode clientResponse) throws IOException {
        String requestUrl = transactionUrl + "?responseCode=" + clientResponse.getCode();
        logger.debug("Sending commitTransferFlowFiles request to transactionUrl: {}", requestUrl);

        HttpDelete delete = createDelete(requestUrl);
        delete.setHeader("Accept", "application/json");
        delete.setHeader(HttpHeaders.PROTOCOL_VERSION, String.valueOf(transportProtocolVersionNegotiator.getVersion()));

        setHandshakeProperties(delete);

        try (CloseableHttpResponse response = getHttpClient().execute(delete)) {
            int responseCode = response.getStatusLine().getStatusCode();
            logger.debug("commitTransferFlowFiles responseCode={}", responseCode);

            try (InputStream content = response.getEntity().getContent()) {
                switch (responseCode) {
                    case RESPONSE_CODE_OK :
                        return readResponse(content);

                    case RESPONSE_CODE_BAD_REQUEST :
                        return readResponse(content);

                    default:
                        throw handleErrResponse(responseCode, content);
                }
            }
        }

    }

}

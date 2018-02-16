package org.apache.nifi.remote;

import org.apache.nifi.attribute.expression.language.PreparedQuery;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class PeerDescriptionModifier {

    public enum RequestType {
        SiteToSiteDetail,
        Peers
    }

    private static class Target {
        private String name;
        private PreparedQuery predicate;
        private String hostname;
        private Integer port;
        private Boolean secure;
    }

    // Use TreeMap so that targets are evaluated in alphabetical order.
    private Map<String, Target> routes = new TreeMap<>();


    public static final String PROPERTY_PREFIX = "nifi.remote.route.";

    public void configure(final Properties properties) {
        final Map<String, List<String>> routeDefinitions = properties.stringPropertyNames().stream()
                .filter(k -> k.startsWith(PROPERTY_PREFIX))
                .collect(Collectors.groupingBy(k -> k.substring(PROPERTY_PREFIX.length(), k.lastIndexOf('.'))));

        final Map<Object, Object> collect = routeDefinitions.entrySet().stream().map(r -> {
            final Target target = new Target();
            target.name = r.getKey();
            r.getValue().forEach(k -> {
                final String name = k.substring(k.lastIndexOf('.') + 1);
                final String value = properties.getProperty(k);
                switch (name) {
                    case "when":
                        target.predicate = Query.prepare(value);
                        break;
                    case "hostname":
                        target.hostname = value;
                        break;
                    case "port":
                        target.port = Integer.valueOf(value);
                        break;
                    case "secure":
                        target.secure = Boolean.valueOf(value);
                        break;
                }
            });
            return target;
        }).collect(Collectors.toMap(t -> t.name, t -> t));

        System.out.println(collect);

    }

    /**
     * Modifies target peer description so that subsequent request can go through the appropriate route
     * @param source The source peer from which a request was sent, this can be any server host participated to relay the request,
     *              but should be the one which can contribute to derive the correct target peer.
     * @param target The original target which should receive and process further incoming requests.
     * @param requestType The requested API type.
     * @return A peer description. The original target peer can be returned if there is no intermediate peer such as reverse proxies needed.
     */
    public PeerDescription modify(final PeerDescription source, final PeerDescription target,
                                  final SiteToSiteTransportProtocol protocol, final RequestType requestType) {
        // TODO: Make it configurable. Configuration should be similar to the user identifier mapping at nifi.properties.
        if ("nginx.example.com".equals(source.getHostname()) || "192.168.99.100".equals(source.getHostname())) {
            final int modifiedTargetPort;
            switch (protocol) {
                case RAW:
                    modifiedTargetPort = target.getPort();
                    break;
                case HTTP:
                    switch (target.getPort()) {
                        // Cluster Plain NiFi 0 HTTP
                        case 18080:
                            modifiedTargetPort = 18070;
                            break;
                        // Cluster Plain NiFi 1 HTTP
                        case 18090:
                            modifiedTargetPort = 18071;
                            break;
                        // Cluster Secure NiFi 0 HTTP
                        case 18443:
                            switch (requestType) {
                                case SiteToSiteDetail:
                                    // Back to the source Proxy port (18460 or 18461)
                                    modifiedTargetPort = source.getPort();
                                    break;
                                case Peers:
                                    switch (source.getPort()) {
                                        case 18460: // For cluster-secure-http
                                            modifiedTargetPort = 18470;
                                            break;
                                        case 18461: // For cluster-secure-http-binary
                                            modifiedTargetPort = 18475;
                                            break;
                                        default:
                                            throw new RuntimeException("Unknown source port " + source.getPort());
                                    }
                                    break;
                                default:
                                    throw new RuntimeException("Unknown requestType" + source.getPort());
                            }
                            break;
                        // Cluster Secure NiFi 1 HTTP
                        case 18444:
                            switch (requestType) {
                                case SiteToSiteDetail:
                                    // Back to the source Proxy port (18460 or 18461)
                                    modifiedTargetPort = source.getPort();
                                    break;
                                case Peers:
                                    switch (source.getPort()) {
                                        case 18460: // For cluster-secure-http
                                            modifiedTargetPort = 18471;
                                            break;
                                        case 18461: // For cluster-secure-http-binary
                                            modifiedTargetPort = 18476;
                                            break;
                                        default:
                                            throw new RuntimeException("Unknown source port " + source.getPort());
                                    }
                                    break;
                                default:
                                    throw new RuntimeException("Unknown requestType" + requestType);
                            }
                            break;
                        default:
                            modifiedTargetPort = target.getPort();
                    }
                    break;
                default:
                    throw new RuntimeException("Unknown protocol" + protocol);
            }
            return new PeerDescription("nginx.example.com", modifiedTargetPort, target.isSecure());
        }
        return target;
    }
}

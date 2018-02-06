package org.apache.nifi.remote;

public class PeerDescriptionModifier {

    /**
     * Modifies target peer description so that subsequent request can go through the appropriate route
     * @param source The source peer from which a request was sent, this can be any server host participated to relay the request,
     *              but should be the one which can contribute to derive the correct target peer.
     * @param target The original target which should receive and process further incoming requests.
     * @return A peer description. The original target peer can be returned if there is no intermediate peer such as reverse proxies needed.
     */
    public PeerDescription modify(final PeerDescription source, final PeerDescription target) {
        // TODO: Make it configurable. Configuration should be similar to the user identifier mapping at nifi.properties.
        if ("nginx.example.com".equals(source.getHostname()) || "192.168.99.100".equals(source.getHostname())) {
            final int modifiedTargetPort;
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
                    modifiedTargetPort = 18470;
                    break;
                // Cluster Secure NiFi 1 HTTP
                case 18444:
                    modifiedTargetPort = 18471;
                    break;
                default:
                    modifiedTargetPort = target.getPort();
            }
            return new PeerDescription("nginx.example.com", modifiedTargetPort, target.isSecure());
        }
        return target;
    }
}

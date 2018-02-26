package org.apache.nifi.remote;

import org.apache.nifi.attribute.expression.language.PreparedQuery;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.properties.StandardNiFiProperties;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TestPeerDescriptionModifier {

    @Test
    public void testEL() {
        // final PreparedQuery query = Query.prepare("${literal('hoge')}");
        final PreparedQuery query = Query.prepare("hoge");
        final Map<String, String> valueLookup = new HashMap<>();
        valueLookup.put("protocol", "RAW");
        valueLookup.put("source.hostname", "nginx.example.com");
        valueLookup.put("target.hostname", "localhost");
        final String result = query.evaluateExpressions(valueLookup, null);
        // - Predicate
        // - Target host
        // - Target port
        // - Target secure
        // - If not specified, then use the original value.
        Assert.assertEquals("hoge", result);
    }

    @Test
    public void testProperties() {
        Properties props = new Properties();

        props.put("nifi.remote.route.http.nginx.when", "${source.hostname:equals('nginx.example.com')}");
        props.put("nifi.remote.route.http.nginx.hostname", "nginx.example.com");
        props.put("nifi.remote.route.http.nginx.port", "${target.port}");
        props.put("nifi.remote.route.http.nginx.secure", "true");

        props.put("nifi.remote.route.raw.nginx.when", "hoge");
        props.put("nifi.remote.route.raw.nginx.hostname", "nginx.example.com");
        props.put("nifi.remote.route.raw.nginx.port", "8082");
        props.put("nifi.remote.route.raw.nginx.secure", "true");

        props.put("nifi.remote.input.host", "");
        props.put("nifi.remote.input.secure", "false");
        props.put("nifi.remote.input.socket.port", "18081");
        props.put("nifi.remote.input.http.enabled", "true");
        props.put("nifi.remote.input.http.transaction.ttl", "30 sec");
        props.put("nifi.remote.contents.cache.expiration", "30 secs");


        final NiFiProperties properties = new StandardNiFiProperties(props);
        final PeerDescriptionModifier modifier = new PeerDescriptionModifier(properties);

        final PeerDescription source = new PeerDescription("nginx.example.com", 80, false);
        final PeerDescription target = new PeerDescription("node1", 8080, false);
        final PeerDescription modifiedTarget = modifier.modify(source, target, SiteToSiteTransportProtocol.HTTP, PeerDescriptionModifier.RequestType.SiteToSiteDetail, new HashMap<>());
        Assert.assertNotNull(modifiedTarget);
    }
}

package org.apache.nifi.remote;

import org.apache.nifi.attribute.expression.language.PreparedQuery;
import org.apache.nifi.attribute.expression.language.Query;
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
        final PeerDescriptionModifier modifier = new PeerDescriptionModifier();
        Properties props = new Properties();
        props.put("nifi.remote.route.nginx.when", "hoge");
        props.put("nifi.remote.route.nginx.hostname", "nginx.example.com");
        props.put("nifi.remote.route.nginx.port", "8080");
        props.put("nifi.remote.route.nginx.secure", "true");

        props.put("nifi.remote.route.nginx1.when", "hoge");
        props.put("nifi.remote.route.nginx1.hostname", "nginx.example.com");
        props.put("nifi.remote.route.nginx1.port", "8081");
        props.put("nifi.remote.route.nginx1.secure", "true");

        props.put("nifi.remote.route.nginx2.when", "hoge");
        props.put("nifi.remote.route.nginx2.hostname", "nginx.example.com");
        props.put("nifi.remote.route.nginx2.port", "8082");
        props.put("nifi.remote.route.nginx2.secure", "true");

        props.put("nifi.remote.input.host", "");
        props.put("nifi.remote.input.secure", "false");
        props.put("nifi.remote.input.socket.port", "18081");
        props.put("nifi.remote.input.http.enabled", "true");
        props.put("nifi.remote.input.http.transaction.ttl", "30 sec");
        props.put("nifi.remote.contents.cache.expiration", "30 secs");

        modifier.configure(props);
    }
}

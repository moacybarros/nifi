package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.provenance.ProvenanceEventRecord;

import java.net.URI;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;

/**
 * Analyze a transit URI as a Kafka topic.
 * <li>qualifiedName=topicName@clusterName (example: testTopic@cl1)
 * <li>name=topicName (example: testTopic)
 */
public class KafkaTopic extends AbstractNiFiProvenanceEventAnalyzer {

    private static final String TYPE = "kafka_topic";
    private static final String ATTR_TOPIC = "topic";

    @Override
    public Referenceable analyze(ProvenanceEventRecord event) {
        final Referenceable ref = new Referenceable(TYPE);
        final URI uri = parseUri(event.getTransitUri());
        final String clusterName = clusterResolver.toClusterName(uri.getHost());
        // Remove the heading '/'
        final String topicName = uri.getPath().substring(1);
        ref.set(ATTR_NAME, topicName);
        ref.set(ATTR_TOPIC, topicName);
        ref.set(ATTR_QUALIFIED_NAME, toQualifiedName(clusterName, topicName));
        return ref;
    }

    @Override
    public String targetComponentTypePattern() {
        return "^PublishKafka$";
    }
}

package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.ClusterResolver;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzerFactory;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.when;

public class TestKafkaTopic {

    @Test
    public void test() {
        final String processorName = "PublishKafka";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getComponentType()).thenReturn(processorName);
        when(record.getTransitUri()).thenReturn("PLAINTEXT://kafka.example.com:6667/topicName");

        final ClusterResolver clusterResolver = Mockito.mock(ClusterResolver.class);
        when(clusterResolver.toClusterName(matches("kafka.example.com"))).thenReturn("clusterName");

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, clusterResolver);

        assertNotNull(analyzer);

        final Referenceable ref = analyzer.analyze(record);
        assertEquals("topicName", ref.get(ATTR_NAME));
        assertEquals("topicName", ref.get("topic"));
        assertEquals("topicName@clusterName", ref.get(ATTR_QUALIFIED_NAME));
    }
}

package org.apache.nifi.atlas.processors;

import java.util.HashMap;
import java.util.Map;

public class EgressProcessors {

    private static final Map<String, Egress> processors = new HashMap<>();

    static {
        processors.put("org.apache.nifi.processors.hive.PutHiveStreaming", new PutHiveStreaming());
        processors.put("org.apache.nifi.processors.kafka.pubsub.PublishKafka", new PublishKafka());
        processors.put("org.apache.nifi.processors.kafka.pubsub.PublishKafka_0_10", new PublishKafka());
    }

    public static Egress get(String processorClass) {
        return processors.get(processorClass);
    }
}

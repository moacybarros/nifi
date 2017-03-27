package org.apache.nifi.atlas.processors;

import java.util.HashMap;
import java.util.Map;

public class IngressProcessors {

    private static final Map<String, Ingress> processors = new HashMap<>();

    static {
        processors.put("org.apache.nifi.processors.kafka.pubsub.ConsumeKafka", new ConsumeKafka());
        processors.put("org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_0_10", new ConsumeKafka());
    }

    public static Ingress get(String processorClass) {
        return processors.get(processorClass);
    }
}

package org.apache.nifi.atlas.processors;

import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.atlas.AtlasVariables;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class PublishKafka implements Egress, KafkaTopicDataSetCreator {

    @Override
    public Set<AtlasObjectId> getOutputs(Map<String, String> properties, AtlasVariables atlasVariables) {
        final String topic = properties.get("topic");
        if (topic == null || topic.isEmpty() || ExpressionUtils.isExpressionLanguagePresent(topic)) {
            return null;
        }

        // 'topic' is an unique attribute for 'kafka_topic' type.
        return Collections.singleton(new AtlasObjectId("kafka_topic", "topic", topic));
    }

}

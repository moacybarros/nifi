package org.apache.nifi.atlas.processors;

import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.atlas.AtlasVariables;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ConsumeKafka implements Ingress, KafkaTopicDataSetCreator {

    private Set<String> getTopics(Map<String, String> properties) {
        final String topicString = properties.get("topic");

        if (topicString == null || topicString.isEmpty() || ExpressionUtils.isExpressionLanguagePresent(topicString)) {
            return Collections.emptySet();
        }

        return Arrays.stream(topicString.split(","))
                .filter(s -> s != null && !s.isEmpty()).collect(Collectors.toSet());
    }

    @Override
    public Set<AtlasObjectId> getInputs(Map<String, String> properties, AtlasVariables atlasVariables) {
        final Set<AtlasObjectId> objectIds = new HashSet<>();
        for (String topic : getTopics(properties)) {
            // 'topic' is an unique attribute for 'kafka_topic' type.
            objectIds.add(new AtlasObjectId("kafka_topic", "topic", topic));
        }

        return objectIds;
    }

}

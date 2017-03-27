package org.apache.nifi.atlas.processors;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;

import java.util.Map;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;

public interface KafkaTopicDataSetCreator extends DataSetEntityCreator {
    @Override
    default AtlasEntity create(AtlasObjectId objectId, Map<String, String> properties) {
        final Object topic = objectId.getUniqueAttributes().get("topic");
        final AtlasEntity entity = new AtlasEntity();
        entity.setTypeName("kafka_topic");
        entity.setAttribute(ATTR_NAME, topic);
        entity.setAttribute(ATTR_QUALIFIED_NAME, topic);
        entity.setAttribute("topic", topic);
        entity.setAttribute("uri", properties.get("bootstrap.servers"));
        entity.setVersion(1L);
        return entity;
    }
}

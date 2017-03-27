package org.apache.nifi.atlas.processors;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;

import java.util.Map;

public interface DataSetEntityCreator {
    AtlasEntity create(AtlasObjectId objectId, Map<String, String> properties);
}

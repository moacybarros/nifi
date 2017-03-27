package org.apache.nifi.atlas.processors;

import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.atlas.AtlasVariables;
import org.apache.nifi.atlas.NiFiTypes;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;

public class PutHiveStreaming implements Egress {

    @Override
    public Set<AtlasObjectId> getOutputs(Map<String, String> properties, AtlasVariables atlasVariables) {
        final String database = properties.get("hive-stream-database-name");
        if (isEmpty(database)) {
            return null;
        }

        final String table = properties.get("hive-stream-table-name");
        if (isEmpty(database)) {
            return null;
        }

        final String qualifiedName = String.format("%s.%s@%s", database, table, atlasVariables.getAtlasClusterName());

        return Collections.singleton(new AtlasObjectId("hive_table", ATTR_QUALIFIED_NAME, qualifiedName));
    }

}

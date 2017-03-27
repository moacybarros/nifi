package org.apache.nifi.atlas.processors;

import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.atlas.AtlasVariables;

import java.util.Map;
import java.util.Set;

public interface Egress {

    Set<AtlasObjectId> getOutputs(Map<String, String> processorProperties, AtlasVariables atlasVariables);

}

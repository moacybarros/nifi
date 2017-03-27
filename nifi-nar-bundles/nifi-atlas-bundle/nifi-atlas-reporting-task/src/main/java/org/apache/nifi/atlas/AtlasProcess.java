package org.apache.nifi.atlas;

import org.apache.atlas.model.instance.AtlasObjectId;

import java.util.Set;

/**
 * This interface represents 'Process' type in Atlas type system
 * which has inputs and outputs attribute referring 'DataSet' entities.
 */
public interface AtlasProcess {
    Set<AtlasObjectId> getInputs();
    Set<AtlasObjectId> getOutputs();
}

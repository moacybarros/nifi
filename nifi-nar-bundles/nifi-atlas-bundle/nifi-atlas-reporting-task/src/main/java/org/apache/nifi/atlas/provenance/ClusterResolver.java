package org.apache.nifi.atlas.provenance;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;

import java.util.Collection;
import java.util.Collections;

public interface ClusterResolver {

    default Collection<ValidationResult> validate(final ValidationContext validationContext) {
        return Collections.emptySet();
    }

    void configure(PropertyContext context);

    // TODO: add default cluster nama capability.
    String toClusterName(String hostname);

}

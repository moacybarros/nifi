package org.apache.nifi.atlas.provenance;

import org.apache.atlas.typesystem.Referenceable;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public class DataSetRefs {
    private Set<Referenceable> inputs;
    private Set<Referenceable> outputs;

    public Set<Referenceable> getInputs() {
        return inputs != null ? inputs : Collections.emptySet();
    }

    public void addInput(Referenceable input) {
        if (inputs == null) {
            inputs = new LinkedHashSet<>();
        }
        inputs.add(input);
    }

    public void setInputs(Set<Referenceable> inputs) {
        this.inputs = inputs;
    }

    public Set<Referenceable> getOutputs() {
        return outputs != null ? outputs : Collections.emptySet();
    }

    public void setOutputs(Set<Referenceable> outputs) {
        this.outputs = outputs;
    }

    public void addOutput(Referenceable output) {
        if (outputs == null) {
            outputs = new LinkedHashSet<>();
        }
        outputs.add(output);
    }

    public boolean isEmpty() {
        return (inputs == null || inputs.isEmpty()) && (outputs == null || outputs.isEmpty());
    }
}

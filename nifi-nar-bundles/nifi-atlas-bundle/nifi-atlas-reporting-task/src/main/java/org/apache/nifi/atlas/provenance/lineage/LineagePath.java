/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.atlas.provenance.lineage;

import org.apache.nifi.atlas.NiFiFlowPath;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.provenance.ProvenanceEventRecord;

import java.util.ArrayList;
import java.util.List;

public class LineagePath {
    private List<ProvenanceEventRecord> events = new ArrayList<>();
    private List<LineagePath> parents = new ArrayList<>();
    private DataSetRefs refs;

    /**
     * NOTE: The list contains provenance events in reversed order, i.e. the last one first.
     */
    public List<ProvenanceEventRecord> getEvents() {
        return events;
    }

    public List<LineagePath> getParents() {
        return parents;
    }

    public DataSetRefs getRefs() {
        return refs;
    }

    public void setRefs(DataSetRefs refs) {
        this.refs = refs;
    }

    public boolean isComplete() {
        return hasInput() && hasOutput();
    }

    public boolean hasInput() {
        return (refs != null && !refs.getInputs().isEmpty()) || parents.stream().anyMatch(parent -> parent.hasInput());
    }

    public boolean hasOutput() {
        return (refs != null && !refs.getOutputs().isEmpty()) || parents.stream().anyMatch(parent -> parent.hasOutput());
    }
}

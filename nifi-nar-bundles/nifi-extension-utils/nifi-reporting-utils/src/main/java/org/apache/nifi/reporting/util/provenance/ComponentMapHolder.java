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
package org.apache.nifi.reporting.util.provenance;

import java.util.HashMap;
import java.util.Map;

public class ComponentMapHolder {
    final Map<String,String> componentMap = new HashMap<>();
    final Map<String,String> componentToParentGroupMap = new HashMap<>();

    public ComponentMapHolder putAll(ComponentMapHolder holder) {
        this.componentMap.putAll(holder.getComponentMap());
        this.componentToParentGroupMap.putAll(holder.getComponentToParentGroupMap());
        return this;
    }

    public Map<String, String> getComponentMap() {
        return componentMap;
    }

    public Map<String, String> getComponentToParentGroupMap() {
        return componentToParentGroupMap;
    }

    public String getComponentName(final String componentId) {
        return componentMap.get(componentId);
    }

    public String getProcessGroupId(final String componentId) {
        return componentToParentGroupMap.get(componentId);
    }

}

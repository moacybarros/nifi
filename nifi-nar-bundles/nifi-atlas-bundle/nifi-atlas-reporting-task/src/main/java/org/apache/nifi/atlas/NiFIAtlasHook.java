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
package org.apache.nifi.atlas;

import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.nifi.atlas.provenance.lineage.LineageContext;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is not thread-safe as it holds uncommitted notification messages within instance.
 * {@link #addMessage(HookNotification.HookNotificationMessage)} and {@link #commitMessages()} should be used serially from a single thread.
 */
public class NiFIAtlasHook extends AtlasHook implements LineageContext {

    private static final String CONF_PREFIX = "atlas.hook.nifi.";
    private static final String HOOK_NUM_RETRIES = CONF_PREFIX + "numRetries";

    @Override
    protected String getNumberOfRetriesPropertyKey() {
        return HOOK_NUM_RETRIES;
    }


    private final List<HookNotification.HookNotificationMessage> messages = new ArrayList<>();

    @Override
    public void addMessage(HookNotification.HookNotificationMessage message) {
        messages.add(message);
    }

    public void commitMessages() {
        try {
            notifyEntities(messages);
        } finally {
            messages.clear();
        }
    }
}

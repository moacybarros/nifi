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

package org.apache.nifi.components.state;

import org.apache.nifi.annotation.behavior.Stateful;

import java.io.IOException;
import java.util.Map;

/**
 * <p>
 * The ExternalStateManager is responsible for providing NiFi a mechanism for retrieving
 * and clearing state stored in an external system the NiFi component interact with.
 * </p>
 *
 * <p>
 * When calling methods in this class, the state is always retrieved/cleared from external system
 * regardless NiFi instance is a part of a cluster or standalone.
 * </p>
 *
 * <p>
 * This mechanism is designed to allow developers to easily store and retrieve small amounts of state.
 * Since implementation of this interface interacts with remote system, one should consider the cost of
 * retrieving this data, and the amount of data should be kept to the minimum required.
 * </p>
 *
 * <p>
 * Any component that wishes to implement ExternalStateManager should also use the {@link Stateful} annotation
 * with {@link Scope#EXTERNAL} to provide a description of what state is being stored.
 * If this annotation is not present, the UI will not expose such information or allow DFMs to clear the state.
 * </p>
 */
public interface ExternalStateManager {

    /**
     * Returns the current state for the component. This return value will never be <code>null</code>.
     *
     * @return the current state for the component
     * @throws IOException if unable to communicate with the underlying storage mechanism
     */
    StateMap getState() throws IOException;

    /**
     * Clears all keys and values from the component's state
     *
     * @throws IOException if unable to communicate with the underlying storage mechanism
     */
    void clear() throws IOException;
}

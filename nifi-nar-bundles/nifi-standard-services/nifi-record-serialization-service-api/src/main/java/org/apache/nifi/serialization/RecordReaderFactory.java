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

package org.apache.nifi.serialization;

import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;

/**
 * <p>
 * A Controller Service that is responsible for creating a {@link RecordReader}.
 * </p>
 */
public interface RecordReaderFactory extends ControllerService {

    /**
     * Create a RecordReader instance to read records from specified InputStream.
     * This is identical to calling {@link #createRecordReader(FlowFile, InputStream, ComponentLog)} with null FlowFile.
     * If this factory requires a FlowFile, throws SchemaNotFoundException.
     * @param in InputStream containing Records
     * @param logger A logger bind to a component
     * @return Created RecordReader instance
     */
    default RecordReader createRecordReader(InputStream in, ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException {
        if (isFlowFileRequired()) {
            throw new SchemaNotFoundException(String.format("%s requires an incoming FlowFile to resolve Record Schema.", this));
        }
        return createRecordReader(null, in, logger);
    }

    /**
     * Create a RecordReader instance to read records from specified InputStream.
     * @param flowFile A FlowFile which is used to resolve Record Schema via Expression Language dynamically.
     *                 This can be null. If this is required, sub classes should implement {@link #isFlowFileRequired()} to return true.
     * @param in InputStream containing Records
     * @param logger A logger bind to a component
     * @return Created RecordReader instance
     */
    RecordReader createRecordReader(FlowFile flowFile, InputStream in, ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException;

    /**
     * @return Whether this factory needs an incoming FlowFile to resolve Record Schema.
     */
    default boolean isFlowFileRequired() {
        return false;
    }

}

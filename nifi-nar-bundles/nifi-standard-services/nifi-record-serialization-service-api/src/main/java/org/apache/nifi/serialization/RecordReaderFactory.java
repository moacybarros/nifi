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

    default RecordReader createRecordReader(InputStream in, ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException {
        return createRecordReader(null, in, logger);
    }

    /**
     * TODO: DOC
     * @param flowFile can be null
     * @param in
     * @param logger
     * @return
     * @throws MalformedRecordException
     * @throws IOException
     * @throws SchemaNotFoundException
     */
    RecordReader createRecordReader(FlowFile flowFile, InputStream in, ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException;

}

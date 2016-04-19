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
package org.apache.nifi.remote.codec;

import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class HttpFlowFileCodec implements FlowFileCodec {

    private long numBytes = -1L;
    private Map<String, String> attributes;

    @Override
    public void encode(final DataPacket dataPacket, final OutputStream outStream) throws IOException {
        setAttributes(dataPacket.getAttributes());
        StreamUtils.copy(dataPacket.getData(), outStream);
        outStream.flush();
    }

    @Override
    public DataPacket decode(final InputStream stream) throws IOException, ProtocolException {
        if(numBytes < 0L) {
            throw new ProtocolException("numBytes should be set based on HTTP request's content-length before decoding input stream.");
        }
        return new StandardDataPacket(attributes, stream, numBytes);
    }

    @Override
    public List<Integer> getSupportedVersions() {
        return null;
    }

    @Override
    public VersionNegotiator getVersionNegotiator() {
        return null;
    }

    @Override
    public String getResourceName() {
        return "HttpFlowFileCodec";
    }

    public void setNumBytes(long numBytes) {
        this.numBytes = numBytes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }
}

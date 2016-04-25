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
package org.apache.nifi.remote.protocol.http;

import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.stream.io.ByteArrayInputStream;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class HttpControlPacket extends StandardDataPacket {

    public static HttpControlPacket createErrorPacket(Throwable t) {
        try {
            byte[] errMsgBytes = t.getMessage().getBytes("UTF-8");
            ByteArrayInputStream bis = new ByteArrayInputStream(errMsgBytes);
            return new HttpControlPacket(new HashMap<>(), bis, errMsgBytes.length);
        } catch (UnsupportedEncodingException e){
            throw new RuntimeException(e);
        }
    }

    public HttpControlPacket(Map<String, String> attributes, InputStream stream, long size) {
        super(attributes, stream, size);
    }

}

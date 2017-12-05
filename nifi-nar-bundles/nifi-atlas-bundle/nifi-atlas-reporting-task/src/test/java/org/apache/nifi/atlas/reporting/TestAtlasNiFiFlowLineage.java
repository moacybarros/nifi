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
package org.apache.nifi.atlas.reporting;

import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.atlas.NiFiTypes;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockValidationContext;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;
import static org.apache.nifi.atlas.reporting.AtlasNiFiFlowLineage.ATLAS_PASSWORD;
import static org.apache.nifi.atlas.reporting.AtlasNiFiFlowLineage.ATLAS_URLS;
import static org.apache.nifi.atlas.reporting.AtlasNiFiFlowLineage.ATLAS_USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAtlasNiFiFlowLineage {

    private final Logger logger = LoggerFactory.getLogger(TestAtlasNiFiFlowLineage.class);

    @Test
    public void validateAtlasUrls() throws Exception {
        final AtlasNiFiFlowLineage reportingTask = new AtlasNiFiFlowLineage();
        final MockProcessContext processContext = new MockProcessContext(reportingTask);
        final MockValidationContext validationContext = new MockValidationContext(processContext);

        processContext.setProperty(ATLAS_USER, "admin");
        processContext.setProperty(ATLAS_PASSWORD, "admin");

        BiConsumer<Collection<ValidationResult>, Consumer<ValidationResult>> assertResults = (rs, a) -> {
            assertTrue(rs.iterator().hasNext());
            for (ValidationResult r : rs) {
                logger.info("{}", r);
                final String subject = r.getSubject();
                if (ATLAS_URLS.getDisplayName().equals(subject)) {
                    a.accept(r);
                }
            }
        };

        // Default setting.
        assertResults.accept(reportingTask.validate(validationContext),
                r -> assertTrue("Atlas URLs is required", !r.isValid()));


        // Invalid URL.
        processContext.setProperty(ATLAS_URLS, "invalid");
        assertResults.accept(reportingTask.validate(validationContext),
                r -> assertTrue("Atlas URLs is invalid", !r.isValid()));

        // Valid URL
        processContext.setProperty(ATLAS_URLS, "http://atlas.example.com:21000");
        assertTrue(processContext.isValid());

        // Valid URL with Expression
        processContext.setProperty(ATLAS_URLS, "http://atlas.example.com:${literal(21000)}");
        assertTrue(processContext.isValid());

        // Valid URLs
        processContext.setProperty(ATLAS_URLS, "http://atlas1.example.com:21000, http://atlas2.example.com:21000");
        assertTrue(processContext.isValid());

        // Invalid and Valid URLs
        processContext.setProperty(ATLAS_URLS, "invalid, http://atlas2.example.com:21000");
        assertResults.accept(reportingTask.validate(validationContext),
                r -> assertTrue("Atlas URLs is invalid", !r.isValid()));
    }

    @Test
    public void testMergeExistingAtlasIds() {
        // These references to DataSets are created by Provenance event analysis.
        final AtlasObjectId fsPath1 = new AtlasObjectId("4a096362-18d2-47f3-b27c-5ea81baac59b", "fs_path",
                Collections.singletonMap(ATTR_QUALIFIED_NAME, "/tmp/1.txt"));
        final AtlasObjectId fsPath2 = new AtlasObjectId("c72cea80-83da-421b-92a2-3ec23446e6ed", "fs_path",
                Collections.singletonMap(ATTR_QUALIFIED_NAME, "/tmp/2.txt"));

        // Queues created by the reporting task do not have GUID assigned yet.
        final AtlasObjectId queue1 = new AtlasObjectId(TYPE_NIFI_QUEUE, ATTR_QUALIFIED_NAME, "20705e20-0160-1000-6de8-04781f599ccc@TEST");
        final AtlasObjectId queue2 = new AtlasObjectId(TYPE_NIFI_QUEUE, ATTR_QUALIFIED_NAME, "20c56bfe-0160-1000-e69e-d698741b1e50@TEST");

        // But some of the queues may already exist in Atlas, if so those have GUID assigned and should be merged.
        final AtlasObjectId queue1Ex = new AtlasObjectId("4226e843-5c60-4d03-b2c6-a9bcdb032682", TYPE_NIFI_QUEUE,
                Collections.singletonMap(ATTR_QUALIFIED_NAME, "20705e20-0160-1000-6de8-04781f599ccc@TEST"));

        // Atlas IDs should be grouped by qualifiedNames regardless of having GUID or not.
        final Map<String, List<AtlasObjectId>> groups = Stream.of(fsPath1, fsPath2, queue1, queue2, queue1Ex).collect(AtlasNiFiFlowLineage.groupByQualifiedName);
        assertEquals(4, groups.size());
        final List<AtlasObjectId> queue1s = groups.get("nifi_queue::20705e20-0160-1000-6de8-04781f599ccc@TEST");
        final List<AtlasObjectId> queue2s = groups.get("nifi_queue::20c56bfe-0160-1000-e69e-d698741b1e50@TEST");
        assertEquals(2, queue1s.size());
        assertEquals(1, queue2s.size());

        // If more than one ids available for a given qualifiedName (merged), then pick the best id.
        final AtlasObjectId queue1BestId = AtlasNiFiFlowLineage.pickBestId.apply(queue1s);
        assertEquals("4226e843-5c60-4d03-b2c6-a9bcdb032682", queue1BestId.getGuid());
        assertEquals("20705e20-0160-1000-6de8-04781f599ccc@TEST", queue1BestId.getUniqueAttributes().get(ATTR_QUALIFIED_NAME));
    }

}

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

import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.atlas.reporting.AtlasNiFiFlowLineage;
import org.apache.nifi.atlas.security.AtlasAuthN;
import org.apache.nifi.atlas.security.Basic;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.atlas.NiFiTypes.NIFI_TYPES;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ITNiFiAtlasClient {
    private static final Logger logger = LoggerFactory.getLogger(ITNiFiAtlasClient.class);
    private NiFiAtlasClient atlasClient;

    @Before
    public void setup() {
        atlasClient = NiFiAtlasClient.getInstance();
        // Add your atlas server ip address into /etc/hosts as atlas.example.com
        PropertyContext propertyContext = mock(PropertyContext.class);
        when(propertyContext.getProperty(AtlasNiFiFlowLineage.ATLAS_USER)).thenReturn(new MockPropertyValue("admin"));
        when(propertyContext.getProperty(AtlasNiFiFlowLineage.ATLAS_PASSWORD)).thenReturn(new MockPropertyValue("admin"));
        final AtlasAuthN atlasAuthN = new Basic();
        atlasAuthN.configure(propertyContext);
        atlasClient.initialize(new String[]{"http://atlas.example.com:21000/"}, atlasAuthN, null);
    }

    @Test
    public void testDeleteTypeDefs() throws Exception {
        atlasClient.deleteTypeDefs(NIFI_TYPES);
    }

    @Test
    public void testRegisterNiFiTypeDefs() throws Exception {
        atlasClient.registerNiFiTypeDefs(true);
    }

    @Test
    public void testSearch() throws Exception {
        final AtlasObjectId atlasObjectId = new AtlasObjectId("kafka_topic", "topic", "nifi-test");
        final AtlasEntity.AtlasEntityWithExtInfo entityDef = atlasClient.searchEntityDef(atlasObjectId);
        logger.info("entityDef={}", entityDef);
    }

    @Test
    public void testSSL() throws Exception {
        System.setProperty("atlas.conf", "/tmp/atlas");
        final Configuration hadoopConf = new Configuration();
        hadoopConf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(hadoopConf);
//        final UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI("atlas/0.hdp.aws.mine@AWS.MINE", "/tmp/atlas/atlas.service.keytab");
        final UserGroupInformation ugi = null;
//        final AtlasClientV2 client = new AtlasClientV2(ugi, null, "https://0.hdp.aws.mine:21443");
        final AtlasClientV2 client = new AtlasClientV2(new String[]{"https://0.hdp.aws.mine:21443"}, new String[]{"admin", "admin"});
        final AtlasObjectId atlasObjectId = new AtlasObjectId("kafka_topic", "topic", "nifi-test");

        final Map<String, String> attributes = new HashMap<>();
        atlasObjectId.getUniqueAttributes().entrySet().stream().filter(entry -> entry.getValue() != null)
                .forEach(entry -> attributes.put(entry.getKey(), entry.getValue().toString()));
        final AtlasEntity.AtlasEntityWithExtInfo entity = client.getEntityByAttribute(atlasObjectId.getTypeName(), attributes);
        logger.info("entity={}", entity);
    }



}

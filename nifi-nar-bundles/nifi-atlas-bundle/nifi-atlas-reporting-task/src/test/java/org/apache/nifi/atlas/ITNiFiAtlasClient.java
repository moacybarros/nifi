package org.apache.nifi.atlas;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.nifi.atlas.NiFiTypes.NIFI_TYPES;


public class ITNiFiAtlasClient {
    private static final Logger logger = LoggerFactory.getLogger(ITNiFiAtlasClient.class);
    private NiFiAtlasClient atlasClient;

    @Before
    public void setup() {
        atlasClient = NiFiAtlasClient.getInstance();
        // Add your atlas server ip address into /etc/hosts as atlas.example.com
        atlasClient.initialize(true, new String[]{"http://atlas.example.com:21000/"}, "admin", "admin", null);
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
        final AtlasObjectId atlasObjectId = new AtlasObjectId("kafka_topic", "topic", "my-topic-01");
        final AtlasEntity.AtlasEntityWithExtInfo entityDef = atlasClient.searchEntityDef(atlasObjectId);
        logger.info("entityDef={}", entityDef);
    }

}

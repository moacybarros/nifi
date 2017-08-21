package org.apache.nifi.atlas;

import org.junit.Test;

public class TestNiFIAtlasHook {

    @Test
    public void test() throws Exception {
        final NiFIAtlasHook hook = new NiFIAtlasHook();
//        hook.sendMessage();
//        hook.createLineageFromKafkaTopic();
        hook.createLineageToKafkaTopic();
    }
}

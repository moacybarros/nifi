package org.apache.nifi.atlas;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ITNiFiFlowAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(ITNiFiFlowAnalyzer.class);

    private NiFiAtlasClient atlasClient;
    private NiFiApiClient nifiClient;
    private AtlasVariables atlasVariables;

    @Before
    public void setup() throws Exception {
        nifiClient = new NiFiApiClient("http://localhost:8080/");

        atlasClient = NiFiAtlasClient.getInstance();
        // Add your atlas server ip address into /etc/hosts as atlas.example.com
        atlasClient.initialize(true, new String[]{"http://atlas.example.com:21000/"}, "admin", "admin", null);

        atlasVariables = new AtlasVariables();
        // TODO: remove when I commit this.
        atlasVariables.setAtlasClusterName("HDP");
    }

    @Test
    public void testFetchNiFiFlow() throws Exception {
        final NiFiFlowAnalyzer flowAnalyzer = new NiFiFlowAnalyzer(nifiClient);
        final NiFiFlow niFiFlow = flowAnalyzer.analyzeProcessGroup(atlasVariables);
        niFiFlow.dump();

        final List<NiFiFlowPath> niFiFlowPaths = flowAnalyzer.analyzePaths(niFiFlow);
        niFiFlowPaths.forEach(path -> logger.info("{} -> {} -> {}",
                path.getIncomingPaths().size(),
                niFiFlow.getProcessors().get(path.getId()).getName(),
                path.getOutgoingPaths().size()));
    }

    @Test
    public void testRegisterNiFiFlow() throws Exception {
        final NiFiFlowAnalyzer flowAnalyzer = new NiFiFlowAnalyzer(nifiClient);
        final NiFiFlow niFiFlow = flowAnalyzer.analyzeProcessGroup(atlasVariables);
        niFiFlow.dump();

        final List<NiFiFlowPath> niFiFlowPaths = flowAnalyzer.analyzePaths(niFiFlow);
        logger.info("nifiFlowPath={}", niFiFlowPaths);

        atlasClient.registerNiFiFlow(niFiFlow, niFiFlowPaths);
    }

}

package org.apache.nifi.atlas.provenance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class NiFiProvenanceEventAnalyzerFactory {

    private static final Logger logger = LoggerFactory.getLogger(NiFiProvenanceEventAnalyzerFactory.class);
    private static final Map<Pattern, NiFiProvenanceEventAnalyzer> analyzers = new ConcurrentHashMap<>();
    private static boolean loaded = false;

    private static void loadAnalyzers() {
        logger.debug("Loading NiFiProvenanceEventAnalyzer ...");
        final ServiceLoader<NiFiProvenanceEventAnalyzer> serviceLoader
                = ServiceLoader.load(NiFiProvenanceEventAnalyzer.class);
        serviceLoader.forEach(analyzer -> {
            final Pattern pattern = Pattern.compile(analyzer.targetComponentTypePattern());
            analyzers.put(pattern, analyzer);
        });
        logger.info("Loaded NiFiProvenanceEventAnalyzers: {}", analyzers);
    }

    /**
     * Find and retrieve NiFiProvenanceEventAnalyzer implementation for the specified className.
     * @param className NiFi components classname.
     * @param clusterResolver Specify a ClusterResolver to use.
     * @return Instance of NiFiProvenanceEventAnalyzer if one is found for the specified className, otherwise null.
     */
    public static NiFiProvenanceEventAnalyzer getAnalyzer(String className, ClusterResolver clusterResolver) {

        if (!loaded) {
            synchronized (analyzers) {
                if (!loaded) {
                    loadAnalyzers();
                    loaded = true;
                }
            }
        }

        NiFiProvenanceEventAnalyzer analyzer = null;
        for (Map.Entry<Pattern, NiFiProvenanceEventAnalyzer> entry : analyzers.entrySet()) {
            if (entry.getKey().matcher(className).matches()) {
                analyzer = entry.getValue();
                break;
            }
        }

        if (analyzer == null) {
            return null;
        }

        analyzer.setClusterResolver(clusterResolver);

        return analyzer;
    }
}

package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Set;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.provenance.analyzer.DatabaseAnalyzerUtil.ATTR_INPUT_TABLES;
import static org.apache.nifi.atlas.provenance.analyzer.DatabaseAnalyzerUtil.ATTR_OUTPUT_TABLES;
import static org.apache.nifi.atlas.provenance.analyzer.DatabaseAnalyzerUtil.parseTableNames;
import static org.apache.nifi.atlas.provenance.analyzer.DatabaseAnalyzerUtil.toTableNameStr;
import static org.apache.nifi.atlas.provenance.analyzer.Hive2JDBC.TYPE_TABLE;

/**
 * Analyze provenance events for PutHiveStreamingProcessor.
 * <li>qualifiedName=dbName@clusterName (example: default@cl1)
 * <li>dbName (example: default)
 */
public class PutHiveStreaming extends AbstractNiFiProvenanceEventAnalyzer {

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {

        final URI uri = parseUri(event.getTransitUri());
        final String clusterName = context.getClusterResolver().fromHostname(uri.getHost());
        final Set<Tuple<String, String>> outputTables = parseTableNames(null, event.getAttribute(ATTR_OUTPUT_TABLES));
        if (outputTables.isEmpty()) {
            return null;
        }

        final DataSetRefs refs = new DataSetRefs(event.getComponentId());
        outputTables.forEach(tableName -> {
            final Referenceable ref = new Referenceable(TYPE_TABLE);
            ref.set(ATTR_NAME, tableName.getValue());
            ref.set(ATTR_QUALIFIED_NAME, toQualifiedName(clusterName, toTableNameStr(tableName)));
            refs.addOutput(ref);
        });
        return refs;
    }

    @Override
    public String targetComponentTypePattern() {
        return "^PutHiveStreaming$";
    }
}

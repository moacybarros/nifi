package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.provenance.ProvenanceEventRecord;

import java.net.URI;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;

/**
 * Analyze a transit URI as a HBase table.
 * <li>qualifiedName=tableName@clusterName (example: myTable@cl1)
 * <li>name=tableName (example: myTable)
 */
public class HBaseTable extends AbstractNiFiProvenanceEventAnalyzer {

    private static final String TYPE = "hbase_table";

    @Override
    public DataSetRefs analyze(ProvenanceEventRecord event) {
        final Referenceable ref = new Referenceable(TYPE);
        final URI uri = parseUri(event.getTransitUri());
        final String clusterName = clusterResolvers.fromHostname(uri.getHost());
        // Remove the heading '/'
        final String tableName = uri.getPath().substring(1);
        ref.set(ATTR_NAME, tableName);
        ref.set(ATTR_QUALIFIED_NAME, toQualifiedName(clusterName, tableName));

        return singleDataSetRef(event.getEventType(), ref);
    }

    @Override
    public String targetTransitUriPattern() {
        return "^hbase://.+$";
    }
}

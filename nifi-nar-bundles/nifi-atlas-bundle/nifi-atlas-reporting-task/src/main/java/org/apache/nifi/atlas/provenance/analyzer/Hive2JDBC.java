package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.provenance.ProvenanceEventRecord;

import java.net.URI;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;

/**
 * Analyze provenance events for Hive2 using JDBC.
 * TODO: Add reserved attributes so that user can specify which table or column is accessed by the query
 * TODO: E.g. hive.input.tableNames=A,B or hive.output.tableNames ... etc, if none of those are known, then we can only create database level lineage.
 * <li>qualifiedName=dbName@clusterName (example: default@cl1)
 * <li>dbName (example: default)
 */
public class Hive2JDBC extends AbstractNiFiProvenanceEventAnalyzer {

    private static final String TYPE = "hive_db";

    @Override
    public Referenceable analyze(ProvenanceEventRecord event) {
        final Referenceable ref = new Referenceable(TYPE);
        final URI uri = parseUri(event.getTransitUri());
        final String clusterName = clusterResolver.toClusterName(uri.getHost());
        // Remove the heading '/'
        final String databaseName = uri.getPath().substring(1);
        ref.set(ATTR_NAME, databaseName);
        ref.set(ATTR_QUALIFIED_NAME, toQualifiedName(clusterName, databaseName));

        return ref;
    }

    @Override
    public String targetTransitUriPattern() {
        return "^jdbc:hive2://.+$";
    }
}

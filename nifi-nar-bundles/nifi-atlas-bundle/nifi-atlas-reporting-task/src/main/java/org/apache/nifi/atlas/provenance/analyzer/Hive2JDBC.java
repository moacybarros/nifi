package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

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

    private static final Logger logger = LoggerFactory.getLogger(Hive2JDBC.class);

    private static final String TYPE_DATABASE = "hive_db";
    private static final String TYPE_TABLE = "hive_table";

    public static final String ATTR_INPUT_TABLES = "query.input.tables";
    public static final String ATTR_OUTPUT_TABLES = "query.output.tables";

    private Set<String> parseTableNames(String tableNamesStr) {
        if (tableNamesStr == null || tableNamesStr.isEmpty()) {
            return Collections.emptySet();
        }
        return Arrays.stream(tableNamesStr.split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
    }

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {

        final Set<String> inputTables = parseTableNames(event.getAttribute(ATTR_INPUT_TABLES));
        final Set<String> outputTables = parseTableNames(event.getAttribute(ATTR_OUTPUT_TABLES));

        // Replace the colon so that the schema in the URI can be parsed correctly.
        final String transitUri = event.getTransitUri().replaceFirst("^jdbc:hive2", "jdbc-hive2");
        final URI uri = parseUri(transitUri);
        final String clusterName = context.getClusterResolver().fromHostname(uri.getHost());
        // TODO: what if uri does not contain database name??
        // Remove the heading '/'
        final String connectedDatabaseName = uri.getPath().substring(1);

        if (inputTables.isEmpty() && outputTables.isEmpty()) {
            // If input/output tables are unknown, create database level lineage.
            return getDatabaseRef(event.getComponentId(), event.getEventType(),
                    clusterName, connectedDatabaseName);
        }

        final DataSetRefs refs = new DataSetRefs(event.getComponentId());
        addRefs(refs, true, event, clusterName, connectedDatabaseName, inputTables);
        addRefs(refs, false, event, clusterName, connectedDatabaseName, outputTables);
        return refs;
    }

    private DataSetRefs getDatabaseRef(String componentId, ProvenanceEventType eventType,
                                       String clusterName, String databaseName) {
        final Referenceable ref = new Referenceable(TYPE_DATABASE);
        ref.set(ATTR_NAME, databaseName);
        ref.set(ATTR_QUALIFIED_NAME, toQualifiedName(clusterName, databaseName));

        return singleDataSetRef(componentId, eventType, ref);
    }

    private void addRefs(DataSetRefs refs, boolean isInput,
                                       ProvenanceEventRecord event, String clusterName,
                                       String connectedDatabaseName, Set<String> tableNames) {
        tableNames.forEach(tableNameStr -> {
            final String[] tableNameSplit = tableNameStr.split("\\.");
            if (tableNameSplit.length != 1 && tableNameSplit.length != 2) {
                logger.warn("Unexpected table name format: {} in {}", new Object[]{tableNameStr, event});
                return;
            }
            final String databaseName = tableNameSplit.length == 2 ? tableNameSplit[0] : connectedDatabaseName;
            final String tableName = tableNameSplit.length == 2 ? tableNameSplit[1] : tableNameSplit[0];
            final Referenceable ref = new Referenceable(TYPE_TABLE);
            ref.set(ATTR_NAME, tableName);
            ref.set(ATTR_QUALIFIED_NAME, toQualifiedName(clusterName, String.format("%s.%s", databaseName, tableName)));
            if (isInput) {
                refs.addInput(ref);
            } else {
                refs.addOutput(ref);
            }
        });
    }

    @Override
    public String targetTransitUriPattern() {
        return "^jdbc:hive2://.+$";
    }
}

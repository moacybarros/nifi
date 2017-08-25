package org.apache.nifi.processors.hive;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.HiveLexer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHiveParser {

    @Test
    public void parseSelect() throws Exception {
        String query = "select a.empid, to_something_awesome(b.saraly) from " +
                "company.emp a inner join default.salary b where a.empid = b.empid";
        final HashSet<TableName> tableNames = findTableNames(query);
        System.out.printf("tableNames=%s\n", tableNames);
        assertEquals(2, tableNames.size());
        assertTrue(tableNames.contains(new TableName("COMPANY", "EMP")));
        assertTrue(tableNames.contains(new TableName("DEFAULT", "SALARY")));
    }

    @Test
    public void parseLongSelect() throws Exception {
        String query = "select\n" +
                "\n" +
                "    i_item_id,\n" +
                "\n" +
                "    i_item_desc,\n" +
                "\n" +
                "    s_state,\n" +
                "\n" +
                "    count(ss_quantity) as store_sales_quantitycount,\n" +
                "\n" +
                "    avg(ss_quantity) as store_sales_quantityave,\n" +
                "\n" +
                "    stddev_samp(ss_quantity) as store_sales_quantitystdev,\n" +
                "\n" +
                "    stddev_samp(ss_quantity) / avg(ss_quantity) as store_sales_quantitycov,\n" +
                "\n" +
                "    count(sr_return_quantity) as store_returns_quantitycount,\n" +
                "\n" +
                "    avg(sr_return_quantity) as store_returns_quantityave,\n" +
                "\n" +
                "    stddev_samp(sr_return_quantity) as store_returns_quantitystdev,\n" +
                "\n" +
                "    stddev_samp(sr_return_quantity) / avg(sr_return_quantity) as store_returns_quantitycov,\n" +
                "\n" +
                "    count(cs_quantity) as catalog_sales_quantitycount,\n" +
                "\n" +
                "    avg(cs_quantity) as catalog_sales_quantityave,\n" +
                "\n" +
                "    stddev_samp(cs_quantity) / avg(cs_quantity) as catalog_sales_quantitystdev,\n" +
                "\n" +
                "    stddev_samp(cs_quantity) / avg(cs_quantity) as catalog_sales_quantitycov\n" +
                "\n" +
                "from\n" +
                "\n" +
                "    store_sales,\n" +
                "\n" +
                "    store_returns,\n" +
                "\n" +
                "    catalog_sales,\n" +
                "\n" +
                "    date_dim d1,\n" +
                "\n" +
                "    date_dim d2,\n" +
                "\n" +
                "    date_dim d3,\n" +
                "\n" +
                "    store,\n" +
                "\n" +
                "    item\n" +
                "\n" +
                "where\n" +
                "\n" +
                "    d1.d_quarter_name = '2000Q1'\n" +
                "\n" +
                "        and d1.d_date_sk = ss_sold_date_sk\n" +
                "\n" +
                "        and i_item_sk = ss_item_sk\n" +
                "\n" +
                "        and s_store_sk = ss_store_sk\n" +
                "\n" +
                "        and ss_customer_sk = sr_customer_sk\n" +
                "\n" +
                "        and ss_item_sk = sr_item_sk\n" +
                "\n" +
                "        and ss_ticket_number = sr_ticket_number\n" +
                "\n" +
                "        and sr_returned_date_sk = d2.d_date_sk\n" +
                "\n" +
                "        and d2.d_quarter_name in ('2000Q1' , '2000Q2', '2000Q3')\n" +
                "\n" +
                "        and sr_customer_sk = cs_bill_customer_sk\n" +
                "\n" +
                "        and sr_item_sk = cs_item_sk\n" +
                "\n" +
                "        and cs_sold_date_sk = d3.d_date_sk\n" +
                "\n" +
                "        and d3.d_quarter_name in ('2000Q1' , '2000Q2', '2000Q3')\n" +
                "\n" +
                "group by i_item_id , i_item_desc , s_state\n" +
                "\n" +
                "order by i_item_id , i_item_desc , s_state\n" +
                "\n" +
                "limit 100";

        final HashSet<TableName> tableNames = findTableNames(query);
        System.out.printf("tableNames=%s\n", tableNames);
        assertEquals(6, tableNames.size());
        assertTrue(tableNames.contains(new TableName(null, "STORE_SALES")));
        assertTrue(tableNames.contains(new TableName(null, "STORE_RETURNS")));
        assertTrue(tableNames.contains(new TableName(null, "CATALOG_SALES")));
        assertTrue(tableNames.contains(new TableName(null, "DATE_DIM")));
        assertTrue(tableNames.contains(new TableName(null, "STORE")));
        assertTrue(tableNames.contains(new TableName(null, "ITEM")));
    }

    @Test
    public void parseUpdate() throws Exception {
        String query = "update table_a set y = 'updated' where x > 100";

        final HashSet<TableName> tableNames = findTableNames(query);
        System.out.printf("tableNames=%s\n", tableNames);
        assertEquals(1, tableNames.size());
        assertTrue(tableNames.contains(new TableName(null, "TABLE_A")));
    }

    @Test
    public void parseDelete() throws Exception {
        String query = "delete from table_a where x > 100";

        final HashSet<TableName> tableNames = findTableNames(query);
        System.out.printf("tableNames=%s\n", tableNames);
        assertEquals(1, tableNames.size());
        assertTrue(tableNames.contains(new TableName(null, "TABLE_A")));
    }

    @Test
    public void parseDDL() throws Exception {
        String query = "CREATE TABLE IF NOT EXISTS EMPLOYEES(\n" +
                "EmployeeID INT,FirstName STRING, Title STRING,\n" +
                "State STRING, Laptop STRING)\n" +
                "COMMENT 'Employee Names'\n" +
                "STORED AS ORC";


        final HashSet<TableName> tableNames = findTableNames(query);
        System.out.printf("tableNames=%s\n", tableNames);
        assertEquals(1, tableNames.size());
        assertTrue(tableNames.contains(new TableName(null, "EMPLOYEES")));
    }

    private HiveParser createParser(String query) {
        final ANTLRStringStream input = new ANTLRStringStream(query.toUpperCase());
        HiveLexer lexer = new HiveLexer(input);
        TokenStream tokens = new CommonTokenStream(lexer);
        return new HiveParser(tokens);
    }

    private static class TableName {
        private final String database;
        private final String table;

        public TableName(String database, String table) {
            this.database = database;
            this.table = table;
        }

        @Override
        public String toString() {
            return "TableName{" +
                    "database='" + database + '\'' +
                    ", table='" + table + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TableName tableName = (TableName) o;

            if (database != null ? !database.equals(tableName.database) : tableName.database != null) return false;
            return table.equals(tableName.table);
        }

        @Override
        public int hashCode() {
            int result = database != null ? database.hashCode() : 0;
            result = 31 * result + table.hashCode();
            return result;
        }
    }

    private HashSet<TableName> findTableNames(final String query) throws RecognitionException {
        final HiveParser parser = createParser(query);
        final HiveParser.statement_return statement = parser.statement();
        final Object treeObj = statement.getTree();
        final HashSet<TableName> tableNames = new HashSet<>();
        findTableNames(treeObj, tableNames);
        return tableNames;
    }

    private void findTableNames(final Object obj, final Set<TableName> tableNames) {
        if (!(obj instanceof CommonTree)) {
            return;
        }
        final CommonTree tree = (CommonTree) obj;
        final int childCount = tree.getChildCount();
        if ("TOK_TABNAME".equals(tree.getText())) {
            switch (childCount) {
                case 1 :
                    tableNames.add(new TableName(null, tree.getChild(0).getText()));
                    break;
                case 2:
                    tableNames.add(new TableName(tree.getChild(0).getText(), tree.getChild(1).getText()));
                    break;
                default:
                    throw new IllegalStateException("TOK_TABNAME does not have expected children, childCount=" + childCount);
            }
            return;
        }
        for (int i = 0; i < childCount; i++) {
            findTableNames(tree.getChild(i), tableNames);
        }
    }

}

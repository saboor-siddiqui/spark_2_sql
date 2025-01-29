package com.dataframe.converter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.dataframe.parser.DataFrameAPICodeParser;
import com.dataframe.parser.DataFrameNode;

public class DataFrameToSQLConverter {
    private final StringBuilder sqlQuery;
    private final List<String> selectColumns;
    private final Set<Join> joins;
    private final Set<String> whereClauses; // Changed to Set to prevent duplicates
    private String fromClause;
    private String orderByClause;
    private String limitClause;
    private String groupByClause;
    private String havingClause;
    private String windowFunctionClause;
    private boolean isDistinct;

    public DataFrameToSQLConverter() {
        this.sqlQuery = new StringBuilder();
        this.selectColumns = new ArrayList<>();
        this.joins = new HashSet<>();
        this.whereClauses = new HashSet<>(); // Changed to HashSet
        this.isDistinct = false;
    }

    public String convert(DataFrameNode rootNode, String tableName) {
        resetState();
        if (rootNode == null) {
            throw new IllegalArgumentException("Root node cannot be null. Ensure the DataFrame code is correctly formatted.");
        }
        fromClause = String.format(" FROM `%s`", tableName);
        traverseTree(rootNode);
        buildSQLQuery();
        return sqlQuery.toString();
    }

    private void resetState() {
        sqlQuery.setLength(0);
        selectColumns.clear();
        joins.clear();
        whereClauses.clear();
        fromClause = null;
        orderByClause = null;
        limitClause = null;
        groupByClause = null;
        havingClause = null;
        windowFunctionClause = null;
        isDistinct = false;
    }

    private void traverseTree(DataFrameNode node) {
        if (node == null) return;

        // Process children first to handle dependencies
        for (DataFrameNode child : node.getChildren()) {
            traverseTree(child);
        }

        // Process current node
        processNode(node);
    }

    private void processNode(DataFrameNode node) {
        switch (node.getType()) {
            case "select":
                handleSelect(node);
                break;
            case "join":
                handleJoin(node);
                break;
            case "filter":
                handleFilter(node);
                break;
            case "orderBy":
                handleOrderBy(node);
                break;
            case "limit":
                handleLimit(node);
                break;
            case "groupBy":
                handleGroupBy(node);
                break;
            case "having":
                handleHaving(node);
                break;
            case "windowFunction":
                handleWindowFunction(node);
                break;
            case "distinct":
                isDistinct = true;
                break;
            case "from":
                // Skip from node as table name is provided separately
                break;
            case "withColumn":
                Map<String, Object> details = node.getDetails();
                String newCol = (String) details.get("newColumn");
                String expr = (String) details.get("expression");
                selectColumns.add(expr + " AS " + newCol);
                break;
            case "withColumnRenamed":
                Map<String, Object> renameDetails = node.getDetails();
                String oldCol = (String) renameDetails.get("oldColumn");
                String newName = (String) renameDetails.get("newColumn");
                selectColumns.add(oldCol + " AS " + newName);
                break;
            default:
                throw new IllegalArgumentException("Unknown node type: " + node.getType());
        }
    }

    private void handleSelect(DataFrameNode node) {
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) node.getDetails().get("columns");
        selectColumns.addAll(columns);
    }

    private void handleJoin(DataFrameNode node) {
        String table = (String) node.getDetails().get("table");
        String condition = (String) node.getDetails().get("condition");
        String joinType = (String) node.getDetails().getOrDefault("joinType", "INNER");
        joins.add(new Join(table, condition, joinType));
    }

    private void handleFilter(DataFrameNode node) {
        if (node.getDetails() != null && node.getDetails().containsKey("condition")) {
            String condition = (String) node.getDetails().get("condition");
            // Clean up the condition and add to whereClauses
            condition = condition.replaceAll("^\"|\"$", "").trim();
            whereClauses.add(condition);
        }
    }

    private void handleOrderBy(DataFrameNode node) {
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) node.getDetails().get("columns");
        // Normalize desc syntax
        orderByClause = String.join(", ", columns)
            .replaceAll("desc\\((.*?)\\)", "$1 desc");
    }

    private void handleLimit(DataFrameNode node) {
        limitClause = node.getDetails().get("value").toString();
    }

    private void handleGroupBy(DataFrameNode node) {
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) node.getDetails().get("columns");
        groupByClause = String.join(", ", columns);
    }

    private void handleHaving(DataFrameNode node) {
        havingClause = (String) node.getDetails().get("condition");
    }

    private void handleWindowFunction(DataFrameNode node) {
        windowFunctionClause = (String) node.getDetails().get("function");
    }

    private void buildSQLQuery() {
        sqlQuery.append("SELECT ");
        
        if (isDistinct) {
            sqlQuery.append("DISTINCT ");
        }
        
        if (windowFunctionClause != null) {
            selectColumns.add(windowFunctionClause);
        }
        
        sqlQuery.append(String.join(", ", selectColumns));
        sqlQuery.append(fromClause);

        for (Join join : joins) {
            sqlQuery.append(" ").append(join);
        }

        if (!whereClauses.isEmpty()) {
            sqlQuery.append(" WHERE ").append(String.join(" AND ", whereClauses));
        }

        if (groupByClause != null) {
            sqlQuery.append(" GROUP BY ").append(groupByClause);
        }

        if (havingClause != null) {
            sqlQuery.append(" HAVING ").append(havingClause);
        }

        if (orderByClause != null) {
            sqlQuery.append(" ORDER BY ").append(orderByClause);
        }

        if (limitClause != null) {
            sqlQuery.append(" LIMIT ").append(limitClause);
        }
    }

    private static class Join {
        private final String table;
        private final String condition;
        private final String joinType;

        Join(String table, String condition, String joinType) {
            this.table = table;
            this.condition = condition;
            this.joinType = joinType;
        }

        @Override
        public String toString() {
            String joinTypeStr = joinType.toUpperCase();
            if (joinTypeStr.equals("INNER JOIN")) {
                joinTypeStr = "INNER"; 
            }
            // Remove backticks around joined table name
            return String.format("%s JOIN %s ON %s", joinTypeStr, table, condition);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Join)) return false;
            Join join = (Join) o;
            return Objects.equals(table, join.table) && 
                   Objects.equals(condition, join.condition) &&
                   Objects.equals(joinType, join.joinType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(table, condition, joinType);
        }
    }

    public static void main(String[] args) {
        DataFrameToSQLConverter converter = new DataFrameToSQLConverter();
        DataFrameAPICodeParser parser = new DataFrameAPICodeParser();
        String tableName = "example_table";
        String sql;
        DataFrameNode root;

        // Example DataFrame API code
        String dataframeCode = "df.groupBy(\"id\").count()";

        // Parse the DataFrame API code into operations
        root = parser.parse(dataframeCode);
        if (root == null) {
            System.err.println("Parsing failed for code: " + dataframeCode);
        }

        // Convert the operations into SQL
        sql = converter.convert(root, tableName);
        System.out.println("Generated SQL: " + sql);

        // Test other DataFrame API code
        dataframeCode = "df.select(\"name\", \"age\").filter(\"age > 30\")";
        root = parser.parse(dataframeCode);
        if (root == null) {
            System.err.println("Parsing failed for code: " + dataframeCode);
        }
        sql = converter.convert(root, tableName);
        System.out.println("Generated SQL: " + sql);

        dataframeCode = "df.select(\"name\", \"age\").orderBy(\"age DESC\")";
        root = parser.parse(dataframeCode);
        if (root == null) {
            System.err.println("Parsing failed for code: " + dataframeCode);
        }
        sql = converter.convert(root, tableName);
        System.out.println("Generated SQL: " + sql);

        dataframeCode = "df.select(\"category\").distinct()";
        root = parser.parse(dataframeCode);
        if (root == null) {
            System.err.println("Parsing failed for code: " + dataframeCode);
        }
        sql = converter.convert(root, tableName);
        System.out.println("Generated SQL: " + sql);

        dataframeCode = "df.select(\"timestamp\", \"message\").limit(100)";
        root = parser.parse(dataframeCode);
        if (root == null) {
            System.err.println("Parsing failed for code: " + dataframeCode);
        }
        sql = converter.convert(root, tableName);
        System.out.println("Generated SQL: " + sql);

        dataframeCode = "df.select(\"employees.name\", \"departments.name\").join(\"departments\", \"employees.department_id = departments.id\")";
        root = parser.parse(dataframeCode);
        if (root == null) {
            System.err.println("Parsing failed for code: " + dataframeCode);
        }
        sql = converter.convert(root, tableName);
        System.out.println("Generated SQL: " + sql);

        dataframeCode = "myDf.select(\"sales.date\", \"customers.name\") " +
      ".join(\"customers\", \"sales.customer_id = customers.id\") " +
      ".groupBy(\"date\") " +
      ".agg(sum(\"amount\").as(\"total_sales\")) " +
      ".orderBy(desc(\"total_sales\")) " +
      ".limit(10)";

        root = parser.parse(dataframeCode);
        if (root == null) {
            System.err.println("Parsing failed for code: " + dataframeCode);
        }
        sql = converter.convert(root, tableName);
        System.out.println("Generated SQL: " + sql);

        // dataframeCode = "df.select(\"sales.date\", \"customers.name\").join(\"customers\", \"sales.customer_id = customers.id\").groupBy(\"date\").agg(sum(\"amount\").as(\"total_sales\")).orderBy(desc(\"total_sales\")).limit(10)";
        // root = parser.parse(dataframeCode);
        // if (root == null) {
        //     System.err.println("Parsing failed for code: " + dataframeCode);
        // }
        // sql = converter.convert(root, tableName);
        // System.out.println("Generated SQL: " + sql);

        
    }

}
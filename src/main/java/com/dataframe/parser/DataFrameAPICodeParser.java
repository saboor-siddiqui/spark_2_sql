package com.dataframe.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DataFrameAPICodeParser {
    private static final String TABLE_PREFIX = "axp-lumid.dw_anon.";

    // Add new patterns
    private static final Pattern WITH_COLUMN_PATTERN = 
        Pattern.compile("\\.withColumn\\(\"(.*?)\",\\s*(.*?)\\)");
    private static final Pattern WITH_COLUMN_RENAMED_PATTERN = 
        Pattern.compile("\\.withColumnRenamed\\(\"(.*?)\",\\s*\"(.*?)\"\\)");

    public DataFrameNode parse(String dataframeCode) {
        // 1) Remove any "val something = " prefix
        dataframeCode = dataframeCode.replaceAll("val\\s+\\S+\\s*=\\s*", "");

        // 2) Normalize whitespace and newlines
        dataframeCode = dataframeCode.replaceAll("\\s+", " ").trim();

        DataFrameNode root = null;
        DataFrameNode currentNode = null;

        // Handle .groupBy(...) + .agg(...) style
        if (dataframeCode.matches(".*\\.groupBy\\(\".*\"\\).*\\.agg\\(.*\\).*")) {
            // Extract groupBy column(s)
            List<String> groupCols = extractColumns(dataframeCode, "groupBy");
            // Extract aggregator
            String aggExpr = extractAgg(dataframeCode);

            // FROM node
            String tableName = extractTableName(dataframeCode);
            Map<String, Object> fromOp = createOperation("from", "table", tableName);
            root = new DataFrameNode("from", fromOp, null);
            currentNode = root;

            // SELECT node (include groupBy columns plus aggregated expression)
            List<String> selectCols = new ArrayList<>(groupCols);
            selectCols.add(aggExpr);
            Map<String, Object> selectOp = createOperation("select", "columns", selectCols);
            currentNode = new DataFrameNode("select", selectOp, currentNode);

            // GROUP BY node
            Map<String, Object> groupByOp = createOperation("groupBy", "columns", groupCols);
            currentNode = new DataFrameNode("groupBy", groupByOp, currentNode);
        }

        // Handle .groupBy(...).count()
        if (dataframeCode.matches(".*\\.groupBy\\(\".*\"\\)\\.count\\(\\).*")) {
            String tableName = extractTableName(dataframeCode);
            String columnName = extractColumns(dataframeCode, "groupBy").get(0);

            // Create a FROM node
            Map<String, Object> fromOp = createOperation("from", "table", tableName);
            root = new DataFrameNode("from", fromOp, null);
            currentNode = root;

            // SELECT node (groupBy column plus COUNT(*))
            Map<String, Object> selectOp = createOperation("select", "columns", Arrays.asList(columnName, "COUNT(*)"));
            currentNode = new DataFrameNode("select", selectOp, currentNode);

            // GROUP BY node
            Map<String, Object> groupByOp = createOperation("groupBy", "columns", Arrays.asList(columnName));
            currentNode = new DataFrameNode("groupBy", groupByOp, currentNode);
        }

        // Handle .select(...)
        if (dataframeCode.matches(".*\\.select\\(\".*\"\\).*")) {
            String tableName = extractTableName(dataframeCode);
            List<String> columns = extractColumns(dataframeCode, "select");

            Map<String, Object> fromOp = createOperation("from", "table", tableName);
            if (root == null) {
                root = new DataFrameNode("from", fromOp, null);
                currentNode = root;
            } else {
                currentNode = new DataFrameNode("from", fromOp, currentNode);
            }

            Map<String, Object> selectOp = createOperation("select", "columns", columns);
            currentNode = new DataFrameNode("select", selectOp, currentNode);
        }

        // Handle .filter("...") or .where("...")
        if (dataframeCode.matches(".*\\.(filter|where)\\(\".*\"\\).*")) {
            String condition = extractFilterCondition(dataframeCode);
            if (condition != null && !condition.isEmpty()) {
                Map<String, Object> filterOp = new HashMap<>();
                filterOp.put("condition", condition);
                currentNode = new DataFrameNode("filter", filterOp, currentNode);
            }
        }

        // Handle .join("table", "condition", optional joinType)
        if (dataframeCode.matches(".*\\.join\\(\".*?\",\\s*\".*?\".*\\).*")) {
            Pattern joinPattern = Pattern.compile("\\.join\\(\"(.*?)\",\\s*\"(.*?)\"(,\\s*\"(.*?)\")?\\)");
            Matcher matcher = joinPattern.matcher(dataframeCode);
            while (matcher.find()) {
                String joinTable = TABLE_PREFIX + matcher.group(1);
                String joinCondition = matcher.group(2);
                String joinType = matcher.group(4) != null ? matcher.group(4).toUpperCase() : "INNER";

                Map<String, Object> joinOp = new HashMap<>();
                joinOp.put("table", joinTable);
                joinOp.put("condition", joinCondition);
                joinOp.put("joinType", joinType);
                currentNode = new DataFrameNode("join", joinOp, currentNode);
            }
        }

        // Handle .orderBy(desc(...)) or orderBy("col DESC")
        if (dataframeCode.matches(".*\\.orderBy\\(.*\\).*")) {
            // Example: orderBy(desc("total_sales"))
            // or orderBy("amount DESC")
            List<String> orderByCols = extractOrderByColumns(dataframeCode);
            Map<String, Object> orderByOp = createOperation("orderBy", "columns", orderByCols);
            currentNode = new DataFrameNode("orderBy", orderByOp, currentNode);
        }

        // Handle .distinct()
        if (dataframeCode.matches(".*\\.distinct\\(\\).*")) {
            // Reuse columns from .select(...) for distinct
            List<String> columns = extractColumns(dataframeCode, "select");
            Map<String, Object> distinctOp = createOperation("distinct", "columns", columns);
            currentNode = new DataFrameNode("distinct", distinctOp, currentNode);
        }

        // Handle .limit(n)
        if (dataframeCode.matches(".*\\.limit\\(\\d+\\).*")) {
            int limit = extractLimit(dataframeCode);
            Map<String, Object> limitOp = createOperation("limit", "value", limit);
            currentNode = new DataFrameNode("limit", limitOp, currentNode);
            if (root == null) {
                root = currentNode;
            }
        }

        // Handle .withColumn(...)
        if (dataframeCode.matches(".*\\.withColumn\\(.*\\).*")) {
            Matcher matcher = WITH_COLUMN_PATTERN.matcher(dataframeCode);
            while (matcher.find()) {
                String newColumnName = matcher.group(1);
                String expression = matcher.group(2);
                Map<String, Object> withColumnOp = new HashMap<>();
                withColumnOp.put("newColumn", newColumnName);
                withColumnOp.put("expression", expression);
                currentNode = new DataFrameNode("withColumn", withColumnOp, currentNode);
            }
        }

        // Handle .withColumnRenamed(...)
        if (dataframeCode.matches(".*\\.withColumnRenamed\\(.*\\).*")) {
            Matcher matcher = WITH_COLUMN_RENAMED_PATTERN.matcher(dataframeCode);
            while (matcher.find()) {
                String existingColumn = matcher.group(1);
                String newColumnName = matcher.group(2);
                Map<String, Object> renameOp = new HashMap<>();
                renameOp.put("oldColumn", existingColumn);
                renameOp.put("newColumn", newColumnName);
                currentNode = new DataFrameNode("withColumnRenamed", renameOp, currentNode);
            }
        }

        return root;
    }

    private List<String> extractColumns(String code, String operation) {
        Pattern pattern = Pattern.compile("\\." + operation + "\\((.*?)\\)");
        Matcher matcher = pattern.matcher(code);
        if (matcher.find()) {
            String cols = matcher.group(1);
            // Remove surrounding quotes, split on commas
            cols = cols.replaceAll("\"", "");
            return Arrays.asList(cols.split(",\\s*"));
        }
        return Collections.emptyList();
    }

    // Extract aggregator calls like sum("amount").as("total_sales")
    private String extractAgg(String code) {
        Pattern p = Pattern.compile("\\.agg\\((\\w+)\\(\"(.*?)\"\\)\\.as\\(\"(.*?)\"\\)\\)");
        Matcher m = p.matcher(code);
        if (m.find()) {
            String aggFunction = m.group(1);    // sum, count, etc.
            String column = m.group(2);         // column name
            String alias = m.group(3);          // alias name
            return String.format("%s(%s) as %s", aggFunction, column, alias);
        }
        return "";
    }

    private List<String> extractOrderByColumns(String code) {
        Pattern pattern = Pattern.compile("\\.orderBy\\(desc\\(\"(.*?)\"\\)\\)");
        Matcher matcher = pattern.matcher(code);
        if (matcher.find()) {
            String column = matcher.group(1);
            return Arrays.asList(column + " desc");
        }
        return Collections.emptyList();
    }

    public String extractTableName(String code) {
        // First try to extract table name from spark.read.table("tablename")
        Pattern tablePattern = Pattern.compile("\\.read\\.table\\(\"(.*?)\"\\)");
        Matcher tableMatcher = tablePattern.matcher(code);
        if (tableMatcher.find()) {
            String tableName = tableMatcher.group(1);
            return TABLE_PREFIX + tableName;
        }

        // Fallback: try to get table name from column prefix
        Pattern selectPattern = Pattern.compile("\\.select\\(\"(.*?)\"\\)");
        Matcher selectMatcher = selectPattern.matcher(code);
        if (selectMatcher.find()) {
            String firstCol = selectMatcher.group(1);
            if (firstCol.contains(".")) {
                return TABLE_PREFIX + firstCol.split("\\.")[0];
            }
        }

        return "";
    }

    private String extractCondition(String code, String operation) {
        Pattern pattern = Pattern.compile("\\." + operation + "\\(\"(.*?)\"\\)");
        Matcher matcher = pattern.matcher(code);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    private int extractLimit(String code) {
        Pattern pattern = Pattern.compile("\\.limit\\((\\d+)\\)");
        Matcher matcher = pattern.matcher(code);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        return 0;
    }

    private Map<String, Object> createOperation(String type, String key, Object value) {
        Map<String, Object> op = new HashMap<>();
        if ("join".equals(type)) {
            // Add prefix to join table names
            String tableName = (String) value;
            op.put(key, TABLE_PREFIX + tableName);
        } else if ("select".equals(type)) {
            List<String> columns = (List<String>) value;
            columns = columns.stream()
                .map(col -> col.replaceAll("\"", ""))
                .collect(Collectors.toList());
            op.put(key, columns);
        } else if ("from".equals(type)) {
            // Add prefix to from table names
            String tableName = (String) value;
            op.put(key, TABLE_PREFIX + tableName);
        } else {
            op.put(key, value);
        }
        return op;
    }

    private String extractFilterCondition(String code) {
        Pattern pattern = Pattern.compile("\\.(filter|where)\\(\"(.*?)\"\\)");
        Matcher matcher = pattern.matcher(code);
        if (matcher.find()) {
            // Get the actual filter condition from group 2
            String condition = matcher.group(2);
            // Clean up the condition
            return condition.replaceAll("^\"|\"$", "").trim();
        }
        return null;
    }
}

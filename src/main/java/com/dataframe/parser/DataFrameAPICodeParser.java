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
            String condition = extractCondition(dataframeCode, "(filter|where)");

            Map<String, Object> filterOp = createOperation("filter", "condition", condition);
            currentNode = new DataFrameNode("filter", filterOp, currentNode);
        }

        // Handle .join("table", "condition", optional joinType)
        if (dataframeCode.matches(".*\\.join\\(\".*?\",\\s*\".*?\".*\\).*")) {
            Pattern joinPattern = Pattern.compile("\\.join\\(\"(.*?)\",\\s*\"(.*?)\"(,\\s*\"(.*?)\")?\\)");
            Matcher matcher = joinPattern.matcher(dataframeCode);
            while (matcher.find()) {
                String joinTable = matcher.group(1);
                String joinCondition = matcher.group(2);
                String joinType = matcher.group(4) != null ? matcher.group(4).toUpperCase() : "INNER";

                Map<String, Object> joinOp = createOperation("join", "table", joinTable);
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
        Pattern p = Pattern.compile("\\.agg\\(sum\\(\"(.*?)\"\\)\\.as\\(\"(.*?)\"\\)\\)");
        Matcher m = p.matcher(code);
        if (m.find()) {
            String column = m.group(1);
            String alias = m.group(2);
            return String.format("sum(%s) as %s", column, alias);
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

    private String extractTableName(String code) {
        // Very basic approach: get the first column's prefix as table name
        // For example: "sales.date" → "sales"
        // You can improve this logic as needed
        Pattern p = Pattern.compile("\\.select\\(\"(.*?)\"\\)");
        Matcher m = p.matcher(code);
        if (m.find()) {
            String firstCol = m.group(1);
            if (firstCol.contains(".")) {
                return firstCol.split("\\.")[0];
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
        if ("select".equals(type)) {
            List<String> columns = (List<String>) value;
            columns = columns.stream()
                .map(col -> col.replaceAll("\"", ""))
                .collect(Collectors.toList());
            op.put(key, columns);
        } else if ("orderBy".equals(type)) {
            List<String> columns = (List<String>) value;
            columns = columns.stream()
                .map(col -> col.replaceAll("desc\\(\"(.*?)\"\\)", "$1 desc"))
                .collect(Collectors.toList());
            op.put(key, columns);
        } else {
            op.put(key, value);
        }
        return op;
    }
}

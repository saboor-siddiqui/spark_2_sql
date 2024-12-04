package com.dataframe.parser;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataFrameAPICodeParser {
    public DataFrameNode parse(String dataframeCode) {
        DataFrameNode root = null;
        DataFrameNode currentNode = null;

        // Example: Parse "df.groupBy("id").count()"
        if (dataframeCode.matches(".*\\.groupBy\\(\".*\"\\)\\.count\\(\\).*")) {
            String tableName = extractTableName(dataframeCode);
            String columnName = extractColumnName(dataframeCode, "groupBy");

            Map<String, Object> fromOp = createOperation("from", "table", tableName);
            root = new DataFrameNode("from", fromOp, null);
            currentNode = root;

            Map<String, Object> selectOp = createOperation("select", "columns", Arrays.asList(columnName, "COUNT(*)"));
            currentNode = new DataFrameNode("select", selectOp, currentNode);

            Map<String, Object> groupByOp = createOperation("groupBy", "columns", Arrays.asList(columnName));
            currentNode = new DataFrameNode("groupBy", groupByOp, currentNode);
        }

        // Add more parsing logic for other DataFrame API calls as needed
        if (dataframeCode.matches(".*\\.select\\(\".*\"\\).*")) {
            String tableName = extractTableName(dataframeCode);
            List<String> columns = extractColumns(dataframeCode, "select");

            Map<String, Object> fromOp = createOperation("from", "table", tableName);
            root = new DataFrameNode("from", fromOp, null);
            currentNode = root;

            Map<String, Object> selectOp = createOperation("select", "columns", columns);
            currentNode = new DataFrameNode("select", selectOp, currentNode);
        }

        if (dataframeCode.matches(".*\\.filter\\(\".*\"\\).*")) {
            String condition = extractCondition(dataframeCode, "filter");

            Map<String, Object> filterOp = createOperation("filter", "condition", condition);
            currentNode = new DataFrameNode("filter", filterOp, currentNode);
        }

        if (dataframeCode.matches(".*\\.join\\(\".*\"\\).*")) {
            String joinTable = extractJoinTable(dataframeCode);
            String joinCondition = extractJoinCondition(dataframeCode);
            String joinType = extractJoinType(dataframeCode);

            Map<String, Object> joinOp = createOperation("join", "table", joinTable);
            joinOp.put("condition", joinCondition);
            joinOp.put("joinType", joinType);
            currentNode = new DataFrameNode("join", joinOp, currentNode);
        }

        if (dataframeCode.matches(".*\\.orderBy\\(\".*\"\\).*")) {
            List<String> orderByColumns = extractColumns(dataframeCode, "orderBy");

            Map<String, Object> orderByOp = createOperation("orderBy", "columns", orderByColumns);
            currentNode = new DataFrameNode("orderBy", orderByOp, currentNode);
        }

        if (dataframeCode.matches(".*\\.distinct\\(\\).*")) {
            List<String> columns = extractColumns(dataframeCode, "select");

            Map<String, Object> distinctOp = createOperation("distinct", "columns", columns);
            currentNode = new DataFrameNode("distinct", distinctOp, currentNode);
        }

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

    private String extractTableName(String code) {
        Pattern pattern = Pattern.compile("df\\.(select|join)\\(.*?\\).*?");
        Matcher matcher = pattern.matcher(code);
        if (matcher.find()) {
            // For join operations, extract first table from select
            if (code.contains(".select(")) {
                Pattern selectPattern = Pattern.compile("\\.select\\(\"(.*?)\".*?\\)");
                Matcher selectMatcher = selectPattern.matcher(code);
                if (selectMatcher.find()) {
                    String firstColumn = selectMatcher.group(1).split("\\.")[0];
                    return firstColumn;
                }
            }
        }
        return ""; // Return empty if no table found
    }

    private String extractColumnName(String code, String operation) {
        Pattern pattern = Pattern.compile("\\." + operation + "\\(\"(.*?)\"\\)");
        Matcher matcher = pattern.matcher(code);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    private List<String> extractColumns(String code, String operation) {
        Pattern pattern = Pattern.compile("\\." + operation + "\\((.*?)\\)");
        Matcher matcher = pattern.matcher(code);
        if (matcher.find()) {
            String columns = matcher.group(1);
            // Remove quotes and split by comma
            columns = columns.replaceAll("\"", "");
            return Arrays.asList(columns.split(",\\s*"));
        }
        return Collections.emptyList();
    }

    private String extractCondition(String code, String operation) {
        Pattern pattern = Pattern.compile("\\." + operation + "\\(\"(.*?)\"\\)");
        Matcher matcher = pattern.matcher(code);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    private String extractJoinTable(String code) {
        Pattern pattern = Pattern.compile("\\.join\\(\"(.*?)\"");
        Matcher matcher = pattern.matcher(code);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    private String extractJoinCondition(String code) {
        Pattern pattern = Pattern.compile("\\.join\\(\".*?\",\\s*\"(.*?)\"\\)");
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
        Map<String, Object> operation = new HashMap<>();
        operation.put("type", type);
        operation.put(key, value);
        return operation;
    }

    private String extractJoinType(String code) {
        Pattern pattern = Pattern.compile("\\.join\\(\".*?\",\\s*\".*?\",\\s*\"(.*?)\"\\)");
        Matcher matcher = pattern.matcher(code);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "INNER"; // Default join type
    }
}

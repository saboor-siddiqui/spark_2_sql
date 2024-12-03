package com.dataframe.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataFrameAPICodeParser {
    public List<Map<String, Object>> parse(String dataframeCode) {
        List<Map<String, Object>> operations = new ArrayList<>();
        
        // Example: Parse "df.groupBy("id").count()"
        if (dataframeCode.matches(".*\\.groupBy\\(\".*\"\\)\\.count\\(\\).*")) {
            String tableName = extractTableName(dataframeCode);
            String columnName = extractColumnName(dataframeCode, "groupBy");
            
            operations.add(createOperation("from", "table", tableName));
            operations.add(createOperation("select", "columns", Arrays.asList(columnName, "COUNT(*)")));
            operations.add(createOperation("groupBy", "columns", Arrays.asList(columnName)));
        }
        
        // Add more parsing logic for other DataFrame API calls as needed
        if (dataframeCode.matches(".*\\.select\\(\".*\"\\).*")) {
            String tableName = extractTableName(dataframeCode);
            List<String> columns = extractColumns(dataframeCode, "select");
            
            operations.add(createOperation("from", "table", tableName));
            operations.add(createOperation("select", "columns", columns));
        }
        
        if (dataframeCode.matches(".*\\.filter\\(\".*\"\\).*")) {
            String tableName = extractTableName(dataframeCode);
            String condition = extractCondition(dataframeCode, "filter");
            
            operations.add(createOperation("from", "table", tableName));
            operations.add(createOperation("where", "condition", condition));
        }
        
        if (dataframeCode.matches(".*\\.join\\(\".*\"\\).*")) {
            String tableName = extractTableName(dataframeCode);
            String joinTable = extractJoinTable(dataframeCode);
            String joinCondition = extractJoinCondition(dataframeCode);
            String joinType = extractJoinType(dataframeCode); // Add this line
            
            operations.add(createOperation("from", "table", tableName));
            Map<String, Object> joinOp = new HashMap<>();
            joinOp.put("type", "join");
            joinOp.put("joinType", joinType); // Use extracted join type instead of hardcoding
            joinOp.put("table", joinTable);
            joinOp.put("condition", joinCondition);
            operations.add(joinOp);
        }
        
        if (dataframeCode.matches(".*\\.orderBy\\(\".*\"\\).*")) {
            String tableName = extractTableName(dataframeCode);
            List<String> orderByColumns = extractColumns(dataframeCode, "orderBy");
            
            operations.add(createOperation("from", "table", tableName));
            operations.add(createOperation("orderBy", "columns", orderByColumns));
        }
        
        if (dataframeCode.matches(".*\\.distinct\\(\\).*")) {
            String tableName = extractTableName(dataframeCode);
            List<String> columns = extractColumns(dataframeCode, "select");
            
            operations.add(createOperation("from", "table", tableName));
            operations.add(createOperation("select", "columns", columns));
            operations.add(createOperation("distinct", "columns", columns));
        }
        
        if (dataframeCode.matches(".*\\.limit\\(\\d+\\).*")) {
            String tableName = extractTableName(dataframeCode);
            int limit = extractLimit(dataframeCode);
            
            operations.add(createOperation("from", "table", tableName));
            operations.add(createOperation("limit", "value", limit));
        }
        
        return operations;
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
            String joinType = matcher.group(1).toUpperCase();
            return joinType.equals("INNER") ? "INNER" : joinType + " OUTER";
        }
        return "INNER"; // default join type
    }
}

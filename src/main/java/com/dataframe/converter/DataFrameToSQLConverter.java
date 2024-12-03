package com.dataframe.converter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.dataframe.parser.DataFrameAPICodeParser;

public class DataFrameToSQLConverter {
    private final StringBuilder sqlQuery;
    private String fromClause;
    private final List<String> selectColumns;
    private final List<Join> joins;
    private final List<String> whereClauses;
    
    // Join class to encapsulate join logic
    private static class Join {
        private final String table;
        private final String condition;
        private final String joinType; // Add this field
        
        Join(String table, String condition) {
            this.table = table;
            this.condition = condition;
            this.joinType = "INNER"; // Default join type
        }
        
        Join(String table, String condition, String joinType) {
            this.table = table;
            this.condition = condition;
            this.joinType = joinType;
        }
        
        @Override
        public String toString() {
            String joinTypeStr = joinType.toUpperCase();
            if (joinTypeStr.equals("LEFT") || joinTypeStr.equals("RIGHT")) {
                joinTypeStr += " OUTER";
            }
            return String.format("%s JOIN `%s` ON %s", joinTypeStr, table, condition);
        }
    }
    
    public DataFrameToSQLConverter() {
        this.sqlQuery = new StringBuilder();
        this.selectColumns = new ArrayList<>();
        this.joins = new ArrayList<>();
        this.whereClauses = new ArrayList<>();
    }
    
    private String sanitizeIdentifier(String input) {
        if (input == null) return "";
        // Allow dots for table.column notation
        return input.replaceAll("[^a-zA-Z0-9_.]", "");
    }
    
    private String sanitizeCondition(String input) {
        if (input == null) return "";
        // Allow =, >, < operators and spaces
        return input.replaceAll("[^a-zA-Z0-9_.<>=\\s]", "");
    }
    
    private void appendJoin(Map<String, Object> operation) {
        String table = sanitizeIdentifier((String) operation.get("table"));
        String condition = sanitizeCondition((String) operation.get("condition"));
        String joinType = (String) operation.getOrDefault("joinType", "INNER");
        joins.add(new Join(table, condition, joinType));
    }
    
    public String convert(List<Map<String, Object>> operations) {
        sqlQuery.setLength(0); // Clear previous query
        selectColumns.clear();
        joins.clear();
        whereClauses.clear();
        
        // Process operations
        for (Map<String, Object> op : operations) {
            String type = (String) op.get("type");
            switch (type) {
                case "select":
                    selectColumns.addAll(((List<String>) op.get("columns")).stream()
                        .map(this::sanitizeIdentifier)
                        .collect(Collectors.toList()));
                    break;
                case "from":
                    fromClause = String.format(" FROM `%s`", 
                        sanitizeIdentifier((String) op.get("table")));
                    break;
                case "join":
                    appendJoin(op);
                    break;
                case "where":
                    whereClauses.add(sanitizeCondition((String) op.get("condition")));
                    break;
            }
        }
        
        // Build query
        sqlQuery.append("SELECT ");
        sqlQuery.append(String.join(", ", selectColumns));
        sqlQuery.append(fromClause);
        
        // Append joins
        for (Join join : joins) {
            sqlQuery.append(" ").append(join.toString());
        }
        
        // Append where clauses
        if (!whereClauses.isEmpty()) {
            sqlQuery.append(" WHERE ")
                   .append(String.join(" AND ", whereClauses));
        }
        
        return sqlQuery.toString();
    }

    public static void main(String[] args) {
        DataFrameToSQLConverter converter = new DataFrameToSQLConverter();
        DataFrameAPICodeParser parser = new DataFrameAPICodeParser();
        
        // Example DataFrame API code
        String dataframeCode = "df.groupBy(\"id\").count()";
        
        // Parse the DataFrame API code into operations
        List<Map<String, Object>> operations = parser.parse(dataframeCode);
        
        // Convert the operations into SQL
        String sql = converter.convert(operations);
        System.out.println("Generated SQL: " + sql);
        
        // Test other DataFrame API code
        dataframeCode = "df.select(\"name\", \"age\").filter(\"age > 30\")";
        operations = parser.parse(dataframeCode);
        sql = converter.convert(operations);
        System.out.println("Generated SQL: " + sql);
        
        dataframeCode = "df.select(\"name\", \"age\").orderBy(\"age DESC\")";
        operations = parser.parse(dataframeCode);
        sql = converter.convert(operations);
        System.out.println("Generated SQL: " + sql);
        
        dataframeCode = "df.select(\"category\").distinct()";
        operations = parser.parse(dataframeCode);
        sql = converter.convert(operations);
        System.out.println("Generated SQL: " + sql);
        
        dataframeCode = "df.limit(100)";
        operations = parser.parse(dataframeCode);
        sql = converter.convert(operations);
        System.out.println("Generated SQL: " + sql);

        dataframeCode = "df.select(\"employees.name\", \"departments.name\").join(\"departments\", \"employees.department_id = departments.id\")";
        operations = parser.parse(dataframeCode);
        sql = converter.convert(operations);
        System.out.println("Generated SQL: " + sql);

        dataframeCode = "df.select(\"employees.name\", \"departments.name\").join(\"departments\", \"employees.department_id = departments.id\", \"left\")";
        operations = parser.parse(dataframeCode);
        sql = converter.convert(operations);
        System.out.println("\nTesting LEFT JOIN:");
        System.out.println("Input: " + dataframeCode);
        System.out.println("Generated SQL: " + sql);
    
    }
}
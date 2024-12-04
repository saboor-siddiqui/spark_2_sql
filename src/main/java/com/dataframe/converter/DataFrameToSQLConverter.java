package com.dataframe.converter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.dataframe.parser.DataFrameAPICodeParser;

public class DataFrameToSQLConverter {
    private final StringBuilder sqlQuery;
    private String fromClause;
    private final List<String> selectColumns;
    private final List<Join> joins;
    private final List<String> whereClauses;
    private String groupByClause;
    private String havingClause;
    private String orderByClause;
    private String limitClause;
    private boolean isDistinct;
    
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
            if (joinTypeStr.equals("INNER JOIN")) {
                joinTypeStr = "INNER"; 
            }
            // Remove backticks around joined table name
            return String.format("%s JOIN %s ON %s", joinTypeStr, table, condition);
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
        if (operations == null || operations.isEmpty()) {
            throw new IllegalArgumentException("Operations list cannot be empty");
        }

        resetState();
        
        boolean hasFromClause = false;
        for (Map<String, Object> op : operations) {
            String type = (String) op.get("type");
            if ("from".equals(type)) {
                hasFromClause = true;
                break;
            }
        }
        
        if (!hasFromClause) {
            throw new IllegalArgumentException("Missing FROM clause");
        }

        // Process operations
        for (Map<String, Object> op : operations) {
            String type = (String) op.get("type");
            switch (type) {
                case "select":
                    selectColumns.addAll(((List<String>) op.get("columns")));
                    break;
                case "from":
                    fromClause = String.format(" FROM `%s`", op.get("table"));
                    break;
                case "join":
                    appendJoin(op);
                    break;
                case "where":
                    whereClauses.add((String) op.get("condition"));
                    break;
                case "groupBy":
                    groupByClause = String.join(", ", (List<String>) op.get("columns"));
                    break;
                case "having":
                    havingClause = (String) op.get("condition");
                    break;
                case "orderBy":
                    orderByClause = String.join(", ", (List<String>) op.get("columns"));
                    break;
                case "limit":
                    limitClause = String.valueOf(op.get("value"));
                    break;
                case "distinct":
                    isDistinct = true;
                    break;
                case "window":
                    handleWindowFunction(op);
                    break;
            }
        }

        buildSQLQuery();
        return sqlQuery.toString();
    }

    private void handleWindowFunction(Map<String, Object> op) {
        String function = (String) op.get("function");
        String column = (String) op.get("column");
        List<String> partitionBy = (List<String>) op.get("partitionBy");
        List<String> orderBy = (List<String>) op.get("orderBy");
        String alias = (String) op.get("alias");

        String windowFunc = String.format("%s(%s) OVER (PARTITION BY %s ORDER BY %s) AS %s",
            function, column,
            String.join(", ", partitionBy),
            String.join(", ", orderBy),
            alias);
        
        selectColumns.add(windowFunc);
    }

    private void resetState() {
        sqlQuery.setLength(0); // Clear previous query
        selectColumns.clear();
        joins.clear();
        whereClauses.clear();
        groupByClause = null;
        havingClause = null;
        orderByClause = null;
        limitClause = null;
        isDistinct = false;
    }

    private void buildSQLQuery() {
        sqlQuery.append("SELECT ");
        if (isDistinct) {
            sqlQuery.append("DISTINCT ");
        }
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

        // Append group by clause
        if (groupByClause != null) {
            sqlQuery.append(" GROUP BY ").append(groupByClause);
        }

        // Append having clause
        if (havingClause != null) {
            sqlQuery.append(" HAVING ").append(havingClause);
        }

        // Append order by clause
        if (orderByClause != null) {
            sqlQuery.append(" ORDER BY ").append(orderByClause);
        }

        // Append limit clause
        if (limitClause != null) {
            sqlQuery.append(" LIMIT ").append(limitClause);
        }
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
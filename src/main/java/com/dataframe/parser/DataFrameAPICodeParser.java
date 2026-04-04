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

/**
 * Parser for converting Spark DataFrame operations to SQL equivalent nodes.
 * This class handles the parsing of various DataFrame operations including:
 * - Basic operations (select, filter, limit)
 * - Aggregations (groupBy, count, agg)
 * - Joins
 * - Column operations (withColumn, withColumnRenamed)
 */
public class DataFrameAPICodeParser {
    /** Prefix for all table names in the target database.
     *  Configurable via constructor — defaults to empty string for portability. */
    private final String tablePrefix;

    public DataFrameAPICodeParser() {
        this.tablePrefix = "axp-lumid.dw_anon.";
    }

    public DataFrameAPICodeParser(String tablePrefix) {
        this.tablePrefix = (tablePrefix != null) ? tablePrefix : "";
    }

    /** Regular expression patterns for matching DataFrame operations */
    private static final Pattern WITH_COLUMN_PATTERN =
        Pattern.compile("\\.withColumn\\(\"(.*?)\",\\s*(.*?)\\)");
    private static final Pattern WITH_COLUMN_RENAMED_PATTERN =
        Pattern.compile("\\.withColumnRenamed\\(\"(.*?)\",\\s*\"(.*?)\"\\)");
    // FIX: Capture all args inside select(...) — handles multi-column "col1", "col2"
    private static final Pattern SELECT_PATTERN =
        Pattern.compile("\\.select\\(([^)]+)\\)");
    private static final Pattern FILTER_PATTERN =
        Pattern.compile("\\.(filter|where)\\(\"(.*?)\"\\)");
    private static final Pattern JOIN_PATTERN =
        Pattern.compile("\\.join\\(\"(.*?)\",\\s*\"(.*?)\"(,\\s*\"(.*?)\")?\\)");
    // FIX: Capture all args inside groupBy(...) — same treatment as select
    private static final Pattern GROUP_BY_PATTERN =
        Pattern.compile("\\.groupBy\\(([^)]+)\\)");
    private static final Pattern ORDER_BY_PATTERN =
        Pattern.compile("\\.orderBy\\((desc\\(\"(.*?)\"\\)|\"(.*?)\"(\\s+(?:ASC|DESC))?)\\)");
    private static final Pattern AGG_PATTERN =
        Pattern.compile("\\.agg\\((\\w+)\\(\"(.*?)\"\\)\\.as\\(\"(.*?)\"\\)\\)");
    private static final Pattern LIMIT_PATTERN =
        Pattern.compile("\\.limit\\((\\d+)\\)");
    private static final Pattern TABLE_PATTERN =
        Pattern.compile("\\.read\\.table\\(\"(.*?)\"\\)");
    
    public DataFrameNode parse(String dataframeCode) {
        // Normalize: remove variable assignments and extra whitespace
        dataframeCode = dataframeCode.replaceAll("\\s*=\\s*", "")
                                     .replaceAll("\\s+", " ")
                                     .trim();

        DataFrameNode root = null;
        DataFrameNode currentNode = null;

        // FIX: Use flags to prevent duplicate node creation when multiple
        // pattern branches would otherwise both fire on the same chain.
        boolean handledByGroupByAgg = false;
        boolean handledByGroupByCount = false;

        // Handle .groupBy(...) + .agg(...) style — takes priority over plain select
        if (dataframeCode.matches(".*\\.groupBy\\([^)]+\\).*\\.agg\\(.*\\).*")) {
            handledByGroupByAgg = true;
            List<String> groupCols = extractColumns(dataframeCode, "groupBy");
            String aggExpr = extractAgg(dataframeCode);

            String tableName = extractTableName(dataframeCode);
            Map<String, Object> fromOp = createOperation("from", "table", tableName);
            root = new DataFrameNode("from", fromOp, null);
            currentNode = root;

            List<String> selectCols = new ArrayList<>(groupCols);
            if (!aggExpr.isEmpty()) selectCols.add(aggExpr);
            Map<String, Object> selectOp = createOperation("select", "columns", selectCols);
            currentNode = new DataFrameNode("select", selectOp, currentNode);

            Map<String, Object> groupByOp = createOperation("groupBy", "columns", groupCols);
            currentNode = new DataFrameNode("groupBy", groupByOp, currentNode);
        }

        // Handle .groupBy(...).count() — only if not already handled by groupBy+agg
        if (!handledByGroupByAgg && dataframeCode.matches(".*\\.groupBy\\([^)]+\\)\\.count\\(\\).*")) {
            handledByGroupByCount = true;
            String tableName = extractTableName(dataframeCode);
            List<String> groupCols = extractColumns(dataframeCode, "groupBy");
            String columnName = groupCols.isEmpty() ? "*" : groupCols.get(0);

            Map<String, Object> fromOp = createOperation("from", "table", tableName);
            root = new DataFrameNode("from", fromOp, null);
            currentNode = root;

            Map<String, Object> selectOp = createOperation("select", "columns",
                Arrays.asList(columnName, "COUNT(*)"));
            currentNode = new DataFrameNode("select", selectOp, currentNode);

            Map<String, Object> groupByOp = createOperation("groupBy", "columns", groupCols);
            currentNode = new DataFrameNode("groupBy", groupByOp, currentNode);
        }

        // Handle .select(...) — skip if groupBy branch already created FROM+SELECT
        if (!handledByGroupByAgg && !handledByGroupByCount
                && dataframeCode.matches(".*\\.select\\([^)]+\\).*")) {
            String tableName = extractTableName(dataframeCode);
            List<String> columns = extractColumns(dataframeCode, "select");

            Map<String, Object> fromOp = createOperation("from", "table", tableName);
            root = new DataFrameNode("from", fromOp, null);
            currentNode = root;

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
            Matcher matcher = JOIN_PATTERN.matcher(dataframeCode);
            while (matcher.find()) {
                Map<String, Object> joinOp = new HashMap<>();
                joinOp.put("table", tablePrefix + matcher.group(1));
                joinOp.put("condition", matcher.group(2));
                joinOp.put("joinType", matcher.group(4) != null ? matcher.group(4).toUpperCase() : "INNER");
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
        Pattern pattern = operation.equals("select") ? SELECT_PATTERN : GROUP_BY_PATTERN;
        Matcher matcher = pattern.matcher(code);
        if (matcher.find()) {
            // Strip surrounding quotes from each column name
            String cols = matcher.group(1);
            cols = cols.replaceAll("\"", "").trim();
            return Arrays.stream(cols.split(",\\s*"))
                         .map(String::trim)
                         .filter(c -> !c.isEmpty())
                         .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    private String extractAgg(String code) {
        Matcher m = AGG_PATTERN.matcher(code);
        if (m.find()) {
            String aggFunction = m.group(1);
            String column = m.group(2);
            String alias = m.group(3);
            return String.format("%s(%s) as %s", aggFunction, column, alias);
        }
        return "";
    }

    private List<String> extractOrderByColumns(String code) {
        Matcher matcher = ORDER_BY_PATTERN.matcher(code);
        if (matcher.find()) {
            if (matcher.group(1) != null) {
                // Handle desc(...) case
                return Arrays.asList(matcher.group(2) + " DESC");
            } else {
                // Handle direct column name with optional ASC/DESC
                String column = matcher.group(3);
                String direction = matcher.group(4) != null ? matcher.group(4).trim() : "";
                return Arrays.asList(column + direction);
            }
        }
        return Collections.emptyList();
    }

    public String extractTableName(String code) {
        Matcher tableMatcher = TABLE_PATTERN.matcher(code);
        if (tableMatcher.find()) {
            return tablePrefix + tableMatcher.group(1);
        }

        Matcher selectMatcher = SELECT_PATTERN.matcher(code);
        if (selectMatcher.find()) {
            // Strip quotes and get first column only
            String firstCol = selectMatcher.group(1)
                .replaceAll("\"", "").trim()
                .split(",")[0].trim();
            if (firstCol.contains(".")) {
                return tablePrefix + firstCol.split("\\.")[0];
            }
        }
        return "";
    }

    private String extractFilterCondition(String code) {
        Matcher matcher = FILTER_PATTERN.matcher(code);
        if (matcher.find()) {
            String condition = matcher.group(2);
            return condition.replaceAll("^\"|\"$", "").trim();
        }
        return null;
    }

    private int extractLimit(String code) {
        Matcher matcher = LIMIT_PATTERN.matcher(code);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        return 0;
    }

    /**
     * Creates an operation map with the specified parameters
     * @param type Operation type (from, select, join, etc.)
     * @param key Key for the operation's value
     * @param value Value for the operation
     * @return Map containing the operation details
     */
    private Map<String, Object> createOperation(String type, String key, Object value) {
        Map<String, Object> op = new HashMap<>();
        switch (type) {
            case "join":
            case "from":
                // Only prepend tablePrefix if value is not already prefixed or empty
                String tableVal = (String) value;
                op.put(key, tableVal.startsWith(tablePrefix) ? tableVal : tablePrefix + tableVal);
                break;
            case "select":
                @SuppressWarnings("unchecked")
                List<String> columns = (List<String>) value;
                op.put(key, columns.stream()
                    .map(col -> col.replaceAll("\"", "").trim())
                    .filter(col -> !col.isEmpty())
                    .collect(Collectors.toList()));
                break;
            default:
                op.put(key, value);
        }
        return op;
    }
}

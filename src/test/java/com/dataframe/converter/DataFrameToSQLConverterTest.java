package com.dataframe.converter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DataFrameToSQLConverterTest {
    private DataFrameToSQLConverter converter;
    private List<Map<String, Object>> operations;

    @BeforeEach
    public void setUp() {
        converter = new DataFrameToSQLConverter();
        operations = new ArrayList<>();
    }

    @Test
    public void testMultipleJoins() {
        operations.add(createOperation("from", "table", "orders"));
        operations.add(createOperation("select", "columns", List.of("orders.id", "customers.name", "products.name")));
        
        // First JOIN operation
        Map<String, Object> joinOp1 = new HashMap<>();
        joinOp1.put("type", "join");
        joinOp1.put("joinType", "INNER JOIN");
        joinOp1.put("table", "customers");
        joinOp1.put("condition", "orders.customer_id = customers.id");
        operations.add(joinOp1);
        
        // Second JOIN operation
        Map<String, Object> joinOp2 = new HashMap<>();
        joinOp2.put("type", "join");
        joinOp2.put("joinType", "INNER JOIN");
        joinOp2.put("table", "products");
        joinOp2.put("condition", "orders.product_id = products.id");
        operations.add(joinOp2);
        
        String sql = converter.convert(operations);
        assertEquals(
            "SELECT orders.id, customers.name, products.name FROM `orders` INNER JOIN customers ON orders.customer_id = customers.id INNER JOIN products ON orders.product_id = products.id",
            sql.trim()
        );
    }

    @Test
    public void testSelectWithWhere() {
        operations.add(createOperation("from", "table", "employees"));
        operations.add(createOperation("select", "columns", List.of("name", "salary")));
        operations.add(createOperation("where", "condition", "salary > 50000"));
        
        String sql = converter.convert(operations);
        assertEquals("SELECT name, salary FROM `employees` WHERE salary > 50000", sql.trim());
    }

    @Test
    public void testGroupByWithHaving() {
        operations.add(createOperation("from", "table", "sales"));
        operations.add(createOperation("select", "columns", List.of("department", "COUNT(*)")));
        operations.add(createOperation("groupBy", "columns", List.of("department")));
        operations.add(createOperation("having", "condition", "COUNT(*) > 10"));
        
        String sql = converter.convert(operations);
        assertEquals("SELECT department, COUNT(*) FROM `sales` GROUP BY department HAVING COUNT(*) > 10", sql.trim());
    }

    @Test
    public void testJoinWithOrderBy() {
        // FROM operation
        operations.add(createOperation("from", "table", "orders"));
        
        // SELECT operation
        operations.add(createOperation("select", "columns", List.of("orders.id", "customers.name")));
        
        // JOIN operation
        Map<String, Object> joinOp = new HashMap<>();
        joinOp.put("type", "join");
        joinOp.put("joinType", "INNER JOIN");
        joinOp.put("table", "customers");
        joinOp.put("condition", "orders.customer_id = customers.id");
        operations.add(joinOp);
        
        // ORDER BY operation
        operations.add(createOperation("orderBy", "columns", List.of("orders.id DESC")));
        
        String sql = converter.convert(operations);
        assertEquals(
            "SELECT orders.id, customers.name FROM `orders` INNER JOIN customers ON orders.customer_id = customers.id ORDER BY orders.id DESC",
            sql.trim()
        );
    }

    @Test
    public void testWindowFunction() {
        operations.add(createOperation("from", "table", "employees"));
        operations.add(createOperation("select", "columns", List.of("name", "salary")));
        Map<String, Object> windowOp = new HashMap<>();
        windowOp.put("type", "window");
        windowOp.put("function", "ROW_NUMBER");
        windowOp.put("column", "*");
        windowOp.put("partitionBy", List.of("department"));
        windowOp.put("orderBy", List.of("salary DESC"));
        windowOp.put("alias", "rank");
        operations.add(windowOp);
        
        String sql = converter.convert(operations);
        assertEquals("SELECT name, salary, ROW_NUMBER(*) OVER (PARTITION BY department ORDER BY salary DESC) AS rank FROM `employees`", sql.trim());
    }

    @Test
    public void testDistinct() {
        operations.add(createOperation("from", "table", "products"));
        operations.add(createOperation("select", "columns", List.of("category")));
        operations.add(createOperation("distinct", "columns", List.of("category")));
        
        String sql = converter.convert(operations);
        assertEquals("SELECT DISTINCT category FROM `products`", sql.trim());
    }

    @Test
    public void testLimit() {
        operations.add(createOperation("from", "table", "logs"));
        operations.add(createOperation("select", "columns", List.of("timestamp", "message")));
        operations.add(createOperation("limit", "value", 100));
        
        String sql = converter.convert(operations);
        assertEquals("SELECT timestamp, message FROM `logs` LIMIT 100", sql.trim());
    }

    @Test
    public void testValidation() {
        assertThrows(IllegalArgumentException.class, () -> {
            converter.convert(new ArrayList<>());
        }, "Should throw exception for empty operations");

        assertThrows(IllegalArgumentException.class, () -> {
            operations.add(createOperation("select", "columns", List.of("col1")));
            converter.convert(operations);
        }, "Should throw exception for missing FROM clause");
    }

    private Map<String, Object> createOperation(String type, String key, Object value) {
        Map<String, Object> operation = new HashMap<>();
        operation.put("type", type);
        operation.put(key, value);
        return operation;
    }
}
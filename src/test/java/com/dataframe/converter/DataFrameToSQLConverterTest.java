package com.dataframe.converter;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dataframe.parser.DataFrameNode;

public class DataFrameToSQLConverterTest {
    private DataFrameToSQLConverter converter;

    @BeforeEach
    public void setUp() {
        converter = new DataFrameToSQLConverter();
    }

    @Test
    public void testSelectWithWhere() {
        DataFrameNode root = new DataFrameNode("select", new HashMap<>() {{
            put("columns", List.of("name", "salary"));
        }}, null);
        root.addChild(new DataFrameNode("filter", new HashMap<>() {{
            put("condition", "salary > 50000");
        }}, root));

        String sql = converter.convert(root, "table");
        assertEquals("SELECT name, salary FROM `table` WHERE salary > 50000", sql.trim());
    }

    @Test
    public void testJoinWithOrderBy() {
        DataFrameNode root = new DataFrameNode("select", new HashMap<>() {{
            put("columns", List.of("orders.id", "customers.name"));
        }}, null);
        root.addChild(new DataFrameNode("join", new HashMap<>() {{
            put("table", "customers");
            put("condition", "orders.customer_id = customers.id");
        }}, root));
        root.addChild(new DataFrameNode("orderBy", new HashMap<>() {{
            put("columns", List.of("orders.id DESC"));
        }}, root));

        String sql = converter.convert(root, "orders");
        assertEquals("SELECT orders.id, customers.name FROM `orders` INNER JOIN `customers` ON orders.customer_id = customers.id ORDER BY orders.id DESC", sql.trim());
    }

    @Test
    public void testLimit() {
        DataFrameNode root = new DataFrameNode("select", new HashMap<>() {{
            put("columns", List.of("timestamp", "message"));
        }}, null);
        root.addChild(new DataFrameNode("limit", new HashMap<>() {{
            put("value", 100);
        }}, root));

        String sql = converter.convert(root, "logs");
        assertEquals("SELECT timestamp, message FROM `logs` LIMIT 100", sql.trim());
    }

    @Test
    public void testGroupByWithHaving() {
        DataFrameNode root = new DataFrameNode("select", new HashMap<>() {{
            put("columns", List.of("department", "COUNT(*)"));
        }}, null);
        root.addChild(new DataFrameNode("groupBy", new HashMap<>() {{
            put("columns", List.of("department"));
        }}, root));
        root.addChild(new DataFrameNode("having", new HashMap<>() {{
            put("condition", "COUNT(*) > 10");
        }}, root));

        String sql = converter.convert(root, "table");
        assertEquals("SELECT department, COUNT(*) FROM `table` GROUP BY department HAVING COUNT(*) > 10", sql.trim());
    }

    @Test
    public void testWindowFunction() {
        DataFrameNode root = new DataFrameNode("select", new HashMap<>() {{
            put("columns", List.of("name", "salary", "ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank"));
        }}, null);

        String sql = converter.convert(root, "employees");
        assertEquals("SELECT name, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank FROM `employees`", sql.trim());
    }

    @Test
    public void testDistinct() {
        DataFrameNode root = new DataFrameNode("select", new HashMap<>() {{
            put("columns", List.of("category"));
        }}, null);
        root.addChild(new DataFrameNode("distinct", new HashMap<>(), root));

        String sql = converter.convert(root, "products");
        assertEquals("SELECT DISTINCT category FROM `products`", sql.trim());
    }

    @Test
    public void testMultipleJoins() {
        DataFrameNode root = new DataFrameNode("select", new HashMap<>() {{
            put("columns", List.of("orders.id", "customers.name", "products.name"));
        }}, null);
        root.addChild(new DataFrameNode("join", new HashMap<>() {{
            put("table", "customers");
            put("condition", "orders.customer_id = customers.id");
        }}, root));
        root.addChild(new DataFrameNode("join", new HashMap<>() {{
            put("table", "products");
            put("condition", "orders.product_id = products.id");
        }}, root));

        String sql = converter.convert(root, "orders");
        assertEquals("SELECT orders.id, customers.name, products.name FROM `orders` INNER JOIN `customers` ON orders.customer_id = customers.id INNER JOIN `products` ON orders.product_id = products.id", sql.trim());
    }
}
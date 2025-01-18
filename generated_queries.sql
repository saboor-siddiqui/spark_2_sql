-- Generated SQL Queries
-- Source: /Users/saboor/Documents/Projects/Codes/Spark2SQL/SparkDataFrameExample.scala
-- Generated at: 2025-01-19T00:12:33.351437

SELECT product_id, amount FROM `default_table` WHERE amount > 1000;

SELECT sales.date, customers.name, date, sum(amount) as total_sales FROM `default_table` INNER JOIN `customers` ON sales.customer_id=customers.id GROUP BY date ORDER BY total_sales desc LIMIT 10;

SELECT sales.date, customers.name, date, sum(amount) as total_sales FROM `default_table` INNER JOIN `customers` ON sales.customer_id=customers.id GROUP BY date;

SELECT sales.date, customers.name FROM `default_table` INNER JOIN `customers` ON sales.customer_id=customers.id ORDER BY total_sales desc LIMIT 10;


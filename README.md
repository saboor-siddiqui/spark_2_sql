# Spark2SQL Converter 🚀

A Java library that converts Apache Spark DataFrame API code into equivalent SQL queries. This tool helps developers understand and translate Spark DataFrame operations into standard SQL syntax.

## ✨ Features

Transform Spark DataFrame operations into SQL with support for:

- **Core Operations**
  - `select()` → SELECT statements
  - `filter()/where()` → WHERE clauses 
  - `groupBy()` → GROUP BY clauses
  - `orderBy()` → ORDER BY clauses
  - `distinct()` → DISTINCT keyword

- **Join Operations** 
  - Inner Joins
  - Left Outer Joins
  - Right Outer Joins

- **Advanced Features**
  - Window Functions
  - Aggregate Functions
  - Limit clauses
  - Multiple table joins
  - Column aliasing

## Installation 📦

Clone the repository and build using Maven:

```bash
git clone https://github.com/yourusername/Spark2SQL.git
cd Spark2SQL
mvn clean install
```

## Usage Example

```java
// Create converter instance
DataFrameToSQLConverter converter = new DataFrameToSQLConverter();
DataFrameAPICodeParser parser = new DataFrameAPICodeParser();

// Example DataFrame code
String dataframeCode = "df.select(\"name\", \"age\").filter(\"age > 30\")";

// Parse and convert to SQL
List<Map<String, Object>> operations = parser.parse(dataframeCode);
String sql = converter.convert(operations);

System.out.println(sql);
// Output: SELECT name, age FROM [table](http://_vscodecontentref_/0) WHERE age > 30
```

## 🔍 Example Queries

### Basic Operations

```java
// Simple SELECT
df.select("name", "age")
// → SELECT name, age FROM `table`

// Filtering
df.filter("age > 30")
// → SELECT * FROM `table` WHERE age > 30

// Distinct values
df.select("category").distinct()
// → SELECT DISTINCT category FROM `table`
```

## Joins

```java
// Inner Join
df.select("orders.id", "customers.name")
  .join("customers", "orders.customer_id = customers.id")
// → SELECT orders.id, customers.name 
//   FROM `orders` 
//   INNER JOIN `customers` ON orders.customer_id = customers.id

// Left Outer Join
df.select("employees.name", "departments.name")
  .join("departments", "employees.department_id = departments.id", "left")
// → SELECT employees.name, departments.name 
//   FROM `employees` 
//   LEFT OUTER JOIN `departments` ON employees.department_id = departments.id
```

## 🛠️ Technical Details

- Built with Java 11+
- Uses Maven for dependency management
- Testing framework: JUnit 5
- Code coverage with JaCoCo
- ANTLR4 for parsing
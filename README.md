# Spark2SQL Converter 🚀

> **Convert Apache Spark DataFrame chains to SQL — now powered by a LangGraph multi-agent pipeline.**

A hybrid Java + Python system that turns Spark DataFrame API code (Scala) into clean, validated SQL. Simple chains go through a fast Java path; complex operations (Window functions, PIVOT, nested aggregations, UDFs) are handled by Claude Sonnet via a LangGraph agent graph with automatic validation and repair.

---

## Architecture

```
Input .scala file
      │
      ▼
┌─────────────────┐
│  ExtractorAgent │  ← strips comments, normalises multi-line chains,
│  (Python)       │    resolves val-variable dependencies
└────────┬────────┘
         │  list of op chains
         ▼
┌─────────────────────────────────────────────────────────────┐
│  Router  (is_simple_chain?)                                 │
│                                                             │
│  simple → BridgeCLI (Java JAR subprocess, ~5ms/chain)      │
│  complex → LLM Semantic Parser (Claude Sonnet, structured   │
│            JSON IR → SQL renderer, never raw SQL output)    │
└────────┬────────────────────────────────────────────────────┘
         │  SQL candidate
         ▼
┌─────────────────┐
│ ValidatorAgent  │  ← sqlglot: syntax check + structural completeness
│ (Python)        │    no database required
└────────┬────────┘
         │ invalid ↕ retry (max 3×)
┌─────────────────┐
│  RepairAgent    │  ← Claude Sonnet fixes broken SQL + validation errors
│  (Python/LLM)   │
└────────┬────────┘
         │ valid
         ▼
┌─────────────────┐
│ OptimizerAgent  │  ← sqlglot pretty-print, keyword normalisation,
│ (Python)        │    source-variable comments, trailing semicolons
└────────┬────────┘
         │
         ▼
    output.sql
```

### LangGraph State Graph

```
extractor ──► processing ──► END
```

Each extracted operation runs through: `router → java|llm → validator → (repair loop) → optimizer`

---

## Features

| Category | Operations |
|---|---|
| **Core** | `select`, `filter`/`where`, `distinct`, `limit` |
| **Aggregation** | `groupBy`, `count`, `agg` (sum/avg/count/max/min) |
| **Joins** | INNER, LEFT, RIGHT, FULL OUTER |
| **Ordering** | `orderBy`, `desc(...)`, ASC/DESC |
| **Column ops** | `withColumn`, `withColumnRenamed` |
| **Complex (LLM path)** | Window functions, PIVOT, explode, UDFs, cross joins, nested agg |
| **Auto-repair** | Broken SQL is sent to Claude with the error — fixed in up to 3 attempts |

---

## Quick Start

### 1. Prerequisites

```bash
# Java 11+ (Maven)
mvn --version

# Python 3.10+
python3 --version
```

### 2. Clone & Build the Java JAR

```bash
git clone git@github.com:saboor-siddiqui/spark_2_sql.git
cd spark_2_sql
mvn package -DskipTests
```

### 3. Set Up the Python Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r agents/requirements.txt
pip install langchain-anthropic    # or langchain-openai / langchain-google-genai
```

### 4. Configure API Key

Create a `.env` file in the project root:

```bash
# Anthropic (Claude)
ANTHROPIC_API_KEY=sk-ant-...
SPARK2SQL_LLM_PROVIDER=anthropic
SPARK2SQL_LLM_MODEL=claude-sonnet-4-6

# --- OR OpenAI ---
# OPENAI_API_KEY=sk-...
# SPARK2SQL_LLM_PROVIDER=openai
# SPARK2SQL_LLM_MODEL=gpt-4o

# --- OR Google ---
# GOOGLE_API_KEY=...
# SPARK2SQL_LLM_PROVIDER=google
# SPARK2SQL_LLM_MODEL=gemini-1.5-pro
```

### 5. Convert a Spark File

```bash
# Dry-run (print to stdout)
python -m agents.cli convert path/to/SparkFile.scala --dry-run

# Write to file
python -m agents.cli convert path/to/SparkFile.scala --output output.sql

# Override table prefix and dialect
python -m agents.cli convert SparkFile.scala \
  --table-prefix "mydb.schema." \
  --llm anthropic \
  --model claude-sonnet-4-6 \
  --output output.sql
```

---

## CLI Reference

```
usage: spark2sql convert <file> [options]

positional arguments:
  file                  Path to the .scala Spark file

options:
  -o, --output PATH     Output .sql file (default: <input>.sql)
  --dry-run             Print SQL to stdout instead of writing file
  --llm PROVIDER        LLM provider: openai | anthropic | google (default: openai)
  --model MODEL         Model name (e.g. claude-sonnet-4-6, gpt-4o)
  --table-prefix PREFIX Table prefix for FROM/JOIN (e.g. "mydb.schema.")
  --dialect DIALECT     SQL dialect: bigquery | spark | ansi | mysql | postgres | snowflake
```

---

## Example Conversions

### Simple select + filter
```scala
val result = spark.read.table("users").select("name", "age").filter("age > 30")
```
```sql
-- [JAVA path] result
SELECT name, age
FROM `axp-lumid.dw_anon.users`
WHERE age > 30;
```

### GroupBy + aggregation + orderBy
```scala
val sales = spark.read.table("orders")
  .groupBy("region")
  .agg(sum("revenue").as("total_revenue"))
  .orderBy(desc("total_revenue"))
  .limit(10)
```
```sql
-- [JAVA path] sales
SELECT region, SUM(revenue) AS total_revenue
FROM `axp-lumid.dw_anon.orders`
GROUP BY region
ORDER BY total_revenue DESC
LIMIT 10;
```

### Window function (LLM path)
```scala
val ranked = spark.read.table("employees")
  .select("dept", "name", "salary",
    "rank().over(Window.partitionBy(\"dept\").orderBy(desc(\"salary\"))).as(\"salary_rank\")")
```
```sql
-- [LLM path] ranked
SELECT dept, name, salary,
  RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS salary_rank
FROM `axp-lumid.dw_anon.employees`;
```

### Multi-join complex query (Java + Repair Agent)
```scala
val report = spark.read.table("sales")
  .select("sales.date", "customers.name", "products.category")
  .join("customers", "sales.customer_id = customers.id", "left")
  .join("products",  "sales.product_id = products.id")
  .groupBy("date", "category")
  .agg(sum("amount").as("total"))
  .orderBy(desc("total"))
```
```sql
-- [JAVA path] report
SELECT sales.date, customers.name, products.category, SUM(amount) AS total
FROM `axp-lumid.dw_anon.sales`
LEFT JOIN `axp-lumid.dw_anon.customers` ON sales.customer_id = customers.id
INNER JOIN `axp-lumid.dw_anon.products` ON sales.product_id = products.id
GROUP BY date, category
ORDER BY total DESC;
```

---

## Running Tests

```bash
source .venv/bin/activate

# Python agent tests (no API key required)
python -m pytest agents/tests/ -v

# Java unit tests
mvn test
```

### Test Coverage

| Test file | Cases | What's tested |
|---|---|---|
| `test_extractor.py` | 7 | Comment stripping, multi-line normalization, EOF fix, var inlining |
| `test_validator.py` | 6 | Valid SQL, missing FROM, empty input, SELECT * warning |
| `test_java_bridge.py` | 6 | Simple chain routing, complex op detection (Window, pivot, explode) |
| `test_graph_integration.py` | 3 | Full pipeline, FileNotFoundError handling, result shape |

---

## Project Structure

```
spark_2_sql/
├── agents/                          # Python LangGraph agent system
│   ├── __init__.py
│   ├── cli.py                       # CLI entry point (spark2sql convert ...)
│   ├── config.py                    # Config dataclass with env-var overrides
│   ├── extractor_agent.py           # Scala file → list of op chains
│   ├── java_bridge.py               # BridgeCLI subprocess wrapper + routing
│   ├── llm_parser_agent.py          # LLM → JSON IR → SQL renderer
│   ├── validator_agent.py           # sqlglot-based SQL validation
│   ├── repair_agent.py              # LLM SQL repair with retry guard
│   ├── optimizer_agent.py           # sqlglot pretty-print + normalization
│   ├── graph.py                     # LangGraph StateGraph wiring all agents
│   ├── requirements.txt
│   └── tests/
│       ├── test_extractor.py
│       ├── test_validator.py
│       ├── test_java_bridge.py
│       └── test_graph_integration.py
│
├── src/main/java/com/dataframe/
│   ├── converter/
│   │   ├── BridgeCLI.java           # CLI entry point for Python bridge
│   │   ├── DataFrameToSQLConverter.java
│   │   └── DataFrameCodeExtractor.java
│   └── parser/
│       ├── DataFrameAPICodeParser.java
│       └── DataFrameNode.java
│
├── SparkDataFrameExample.scala      # Example input file
├── SparkDataFrameExample.sql        # Generated output
├── pom.xml
└── .env                             # API keys (gitignored)
```

---

## How the Routing Works

The `JavaBridge.is_simple_chain()` function checks whether a chain contains any of these **complex operation keywords**:

```
pivot  explode  flatMap  mapPartitions
Window  partitionBy  rowNumber  rank  dense_rank
crossJoin  broadcast
```

If **any** of these appear → **LLM path** (Claude Sonnet).  
Otherwise → **Java fast-path** (BridgeCLI subprocess, ~5ms).

If the Java path fails validation → **automatic fallback to LLM**.  
If LLM output fails validation → **Repair Agent** (up to 3 retries).

---

## Configuration

All settings can be overridden via environment variables:

| Variable | Default | Description |
|---|---|---|
| `SPARK2SQL_LLM_PROVIDER` | `openai` | `openai` / `anthropic` / `google` |
| `SPARK2SQL_LLM_MODEL` | `gpt-4o` | Model name for the provider |
| `SPARK2SQL_TABLE_PREFIX` | `axp-lumid.dw_anon.` | Prepended to every FROM/JOIN |
| `OPENAI_API_KEY` | — | OpenAI API key |
| `ANTHROPIC_API_KEY` | — | Anthropic API key |
| `GOOGLE_API_KEY` | — | Google AI API key |

---

## Technical Stack

| Layer | Technology |
|---|---|
| Orchestration | [LangGraph](https://github.com/langchain-ai/langgraph) 0.1+ |
| LLM integration | [LangChain](https://python.langchain.com/) + provider adapters |
| SQL validation | [sqlglot](https://github.com/tobymao/sqlglot) (pure Python, no DB) |
| Java converter | Java 11, Maven, SLF4J |
| Testing | pytest (Python), JUnit 5 (Java) |
| Build | Maven (JAR), pip + venv (Python) |

---

## Known Limitations

- **Scala-only** — PySpark `.py` files not yet supported (planned)
- **No schema inference** — column types are not validated, only SQL syntax
- **JOIN condition parsing** — the Java path can produce malformed `ON` clauses for complex join conditions; the Repair Agent handles these automatically
- **UDFs** — approximated by the LLM (no exact SQL equivalent exists)
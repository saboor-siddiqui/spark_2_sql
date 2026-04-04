import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * AbsurdSparkTest.scala
 *
 * A deliberately extreme Spark DataFrame test file covering:
 *   - Window functions (RANK, DENSE_RANK, ROW_NUMBER, LAG, LEAD, NTILE)
 *   - PIVOT approximation
 *   - Deeply chained multi-join queries (4 tables)
 *   - Self-referencing variable chains
 *   - Nested aggregations with HAVING
 *   - Complex filter expressions (multi-clause AND/OR)
 *   - withColumn + withColumnRenamed stacks
 *   - UDF approximation
 *   - Cross-join
 *   - 10+ operation chains
 */
object AbsurdSparkTest {
  def run(spark: SparkSession): Unit = {

    // -----------------------------------------------------------------------
    // Test 1: Window function — salary rank partitioned by dept + location
    // -----------------------------------------------------------------------
    val salaryRanked = spark.read.table("employees")
      .select(
        "emp_id", "dept", "location", "salary", "hire_date",
        "rank().over(Window.partitionBy(\"dept\", \"location\").orderBy(desc(\"salary\"))).as(\"dept_location_rank\")",
        "dense_rank().over(Window.partitionBy(\"dept\").orderBy(desc(\"salary\"))).as(\"dept_dense_rank\")",
        "row_number().over(Window.partitionBy(\"location\").orderBy(\"hire_date\")).as(\"location_seniority\")"
      )
      .filter("salary > 50000")
      .orderBy("dept", "dept_location_rank")

    // -----------------------------------------------------------------------
    // Test 2: LAG/LEAD — month-over-month revenue comparison
    // -----------------------------------------------------------------------
    val revenueTimeSeries = spark.read.table("monthly_revenue")
      .select(
        "month", "region", "revenue",
        "lag(\"revenue\", 1, 0).over(Window.partitionBy(\"region\").orderBy(\"month\")).as(\"prev_month_revenue\")",
        "lead(\"revenue\", 1, 0).over(Window.partitionBy(\"region\").orderBy(\"month\")).as(\"next_month_revenue\")",
        "(revenue - lag(\"revenue\", 1, 0).over(Window.partitionBy(\"region\").orderBy(\"month\"))) / lag(\"revenue\", 1, 0).over(Window.partitionBy(\"region\").orderBy(\"month\")).as(\"mom_growth_rate\")"
      )
      .filter("revenue > 0")
      .orderBy("region", "month")

    // -----------------------------------------------------------------------
    // Test 3: PIVOT — quarterly revenue by product category
    // -----------------------------------------------------------------------
    val quarterlyPivot = spark.read.table("transactions")
      .groupBy("product_category")
      .pivot("quarter", Seq("Q1", "Q2", "Q3", "Q4"))
      .agg(sum("amount"))
      .orderBy(desc("Q4"))

    // -----------------------------------------------------------------------
    // Test 4: 4-table join with complex conditions and nested aggregation
    // -----------------------------------------------------------------------
    val megaJoinReport = spark.read.table("orders")
      .select(
        "orders.order_id", "orders.order_date", "orders.status",
        "customers.name", "customers.tier", "customers.country",
        "products.category", "products.brand",
        "warehouses.region"
      )
      .join("customers",  "orders.customer_id = customers.id", "left")
      .join("order_items", "orders.order_id = order_items.order_id")
      .join("products",   "order_items.product_id = products.id")
      .join("warehouses", "orders.warehouse_id = warehouses.id", "left")
      .filter("orders.status IN ('COMPLETED', 'SHIPPED') AND customers.tier IN ('GOLD', 'PLATINUM') AND products.category != 'RETURNS'")
      .groupBy("customers.country", "products.category", "warehouses.region")
      .agg(
        sum("order_items.quantity * order_items.unit_price").as("gross_revenue"),
        count("distinct orders.order_id").as("unique_orders"),
        avg("order_items.unit_price").as("avg_unit_price")
      )
      .filter("gross_revenue > 100000")
      .orderBy(desc("gross_revenue"), "customers.country")
      .limit(50)

    // -----------------------------------------------------------------------
    // Test 5: Variable chaining — df2 depends on df1 (dependency inlining)
    // -----------------------------------------------------------------------
    val rawCustomers = spark.read.table("raw_customers")
      .select("id", "email", "signup_date", "country", "plan_type", "mrr")
      .filter("email IS NOT NULL AND mrr > 0")

    val enrichedCustomers = rawCustomers
      .withColumn("annual_revenue", col("mrr") * 12)
      .withColumn("customer_segment",
        when(col("mrr") > 1000, "Enterprise")
          .when(col("mrr") > 200, "Mid-Market")
          .otherwise("SMB")
      )
      .withColumnRenamed("plan_type", "subscription_tier")
      .withColumnRenamed("signup_date", "acquisition_date")

    // -----------------------------------------------------------------------
    // Test 6: NTILE + running total — decile analysis
    // -----------------------------------------------------------------------
    val customerDeciles = spark.read.table("customer_ltv")
      .select(
        "customer_id", "total_ltv", "acquisition_cost",
        "ntile(10).over(Window.orderBy(desc(\"total_ltv\"))).as(\"ltv_decile\")",
        "sum(\"total_ltv\").over(Window.orderBy(desc(\"total_ltv\")).rowsBetween(Window.unboundedPreceding, Window.currentRow)).as(\"running_ltv\")",
        "(total_ltv - acquisition_cost) / acquisition_cost.as(\"roi_multiplier\")"
      )
      .filter("acquisition_cost > 0")
      .orderBy("ltv_decile", desc("total_ltv"))

    // -----------------------------------------------------------------------
    // Test 7: Cross-join for Cartesian product (all region × product combos)
    // -----------------------------------------------------------------------
    val regionProductMatrix = spark.read.table("regions")
      .select("region_id", "region_name", "currency")
      .crossJoin(
        spark.read.table("product_catalog")
          .select("product_id", "product_name", "base_price")
          .filter("is_active = true")
      )
      .withColumn("local_price", col("base_price") * col("fx_rate"))
      .orderBy("region_name", "product_name")

    // -----------------------------------------------------------------------
    // Test 8: UDF approximation — complex string transformations
    // -----------------------------------------------------------------------
    val cleanedEvents = spark.read.table("raw_events")
      .select("event_id", "user_id", "event_type", "raw_payload", "event_ts")
      .withColumn("normalized_event_type", lower(trim(col("event_type"))))
      .withColumn("hour_of_day", hour(col("event_ts")))
      .withColumn("day_of_week", dayofweek(col("event_ts")))
      .withColumn("is_weekend", col("day_of_week").isin(1, 7))
      .withColumn("payload_length", length(col("raw_payload")))
      .filter("normalized_event_type IN ('click', 'purchase', 'view', 'cart_add') AND payload_length < 10000")
      .groupBy("user_id", "normalized_event_type", "hour_of_day")
      .agg(
        count("event_id").as("event_count"),
        countDistinct("event_ts").as("unique_minutes"),
        max("event_ts").as("last_seen")
      )
      .orderBy("user_id", "normalized_event_type", "hour_of_day")

    // -----------------------------------------------------------------------
    // Test 9: Explode — flatten array column into rows
    // -----------------------------------------------------------------------
    val flattenedTags = spark.read.table("articles")
      .select("article_id", "title", "published_at", explode(col("tags")).as("tag"))
      .withColumn("tag_lower", lower(col("tag")))
      .groupBy("tag_lower")
      .agg(
        count("article_id").as("article_count"),
        min("published_at").as("first_used"),
        max("published_at").as("last_used")
      )
      .filter("article_count >= 5")
      .orderBy(desc("article_count"))

    // -----------------------------------------------------------------------
    // Test 10: The "kitchen sink" — 12-operation chain on a financial model
    // -----------------------------------------------------------------------
    val financialModelOutput = spark.read.table("trade_ledger")
      .select(
        "trade_id", "trader_id", "desk", "asset_class",
        "notional_usd", "pnl_usd", "trade_date", "settlement_date",
        "counterparty_id", "book_id", "currency", "strategy_code"
      )
      .join("trader_metadata", "trade_ledger.trader_id = trader_metadata.trader_id")
      .join("desk_limits",     "trade_ledger.desk = desk_limits.desk AND trade_ledger.asset_class = desk_limits.asset_class", "left")
      .join("counterparties",  "trade_ledger.counterparty_id = counterparties.id")
      .join("fx_rates",        "trade_ledger.currency = fx_rates.ccy AND trade_ledger.trade_date = fx_rates.rate_date", "left")
      .withColumn("pnl_local", col("pnl_usd") * col("fx_rate"))
      .withColumn("is_breach", col("notional_usd") > col("desk_limit_usd"))
      .withColumn("days_to_settle", datediff(col("settlement_date"), col("trade_date")))
      .filter("trade_date >= '2024-01-01' AND trade_date < '2025-01-01' AND counterparties.credit_rating IN ('AAA', 'AA+', 'AA') AND is_breach = false")
      .groupBy("desk", "asset_class", "strategy_code", "trader_metadata.region")
      .agg(
        sum("pnl_usd").as("total_pnl_usd"),
        sum("pnl_local").as("total_pnl_local"),
        sum("notional_usd").as("total_notional"),
        count("trade_id").as("trade_count"),
        countDistinct("counterparty_id").as("unique_counterparties"),
        avg("days_to_settle").as("avg_days_to_settle"),
        max("notional_usd").as("max_single_trade"),
        sum("pnl_usd") / sum("notional_usd").as("pnl_per_notional")
      )
      .filter("trade_count >= 10 AND total_pnl_usd > -5000000")
      .withColumnRenamed("trader_metadata.region", "trading_region")
      .orderBy(desc("total_pnl_usd"), "desk", "asset_class")
      .limit(100)

  }
}

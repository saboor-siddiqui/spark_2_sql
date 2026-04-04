-- ================================================================
-- Generated SQL Queries
-- Source:    AbsurdSparkTest.scala
-- Generated: 2026-04-04T08:22:33
-- Provider:  anthropic / claude-sonnet-4-6
-- ================================================================

-- [LLM path] salaryRanked
-- Source: salaryRanked
SELECT
  emp_id,
  dept,
  location,
  salary,
  hire_date,
  RANK() OVER (PARTITION BY dept, location ORDER BY salary DESC) AS dept_location_rank,
  DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS dept_dense_rank,
  ROW_NUMBER() OVER (PARTITION BY location ORDER BY hire_date ASC) AS location_seniority
FROM `axp-lumid.dw_anon.employees`
WHERE
  salary > 50000
ORDER BY
  dept ASC,
  dept_location_rank ASC;

-- [LLM path] revenueTimeSeries
-- Source: revenueTimeSeries
SELECT
  month,
  region,
  revenue,
  LAG(revenue, 1, 0) OVER (PARTITION BY region ORDER BY month) AS prev_month_revenue,
  LEAD(revenue, 1, 0) OVER (PARTITION BY region ORDER BY month) AS next_month_revenue,
  (
    revenue - LAG(revenue, 1, 0) OVER (PARTITION BY region ORDER BY month)
  ) / LAG(revenue, 1, 0) OVER (PARTITION BY region ORDER BY month) AS mom_growth_rate
FROM `axp-lumid.dw_anon.monthly_revenue`
WHERE
  revenue > 0
ORDER BY
  region ASC,
  month ASC;

-- [LLM path] quarterlyPivot
-- Source: quarterlyPivot
-- Note: PIVOT is not standard SQL. Approximated as conditional aggregation.
SELECT
  product_category,
  SUM(CASE WHEN quarter = 'Q1' THEN amount END) AS Q1,
  SUM(CASE WHEN quarter = 'Q2' THEN amount END) AS Q2,
  SUM(CASE WHEN quarter = 'Q3' THEN amount END) AS Q3,
  SUM(CASE WHEN quarter = 'Q4' THEN amount END) AS Q4
FROM `axp-lumid.dw_anon.transactions`
GROUP BY
  product_category
ORDER BY
  Q4 DESC;

-- [JAVA path] megaJoinReport
-- Source variable: megaJoinReport
SELECT
  customers.country,
  products.category,
  warehouses.region
FROM `axp-lumid.dw_anon.orders` AS orders
INNER JOIN `axp-lumid.dw_anon.order_items` AS order_items
  ON orders.order_id = order_items.order_id
LEFT JOIN `axp-lumid.dw_anon.customers` AS customers
  ON orders.customer_id = customers.id
LEFT JOIN `axp-lumid.dw_anon.warehouses` AS warehouses
  ON orders.warehouse_id = warehouses.id
INNER JOIN `axp-lumid.dw_anon.products` AS products
  ON order_items.product_id = products.id
WHERE
  orders.status IN ('COMPLETED', 'SHIPPED')
  AND customers.tier IN ('GOLD', 'PLATINUM')
  AND products.category <> 'RETURNS'
GROUP BY
  customers.country,
  products.category,
  warehouses.region
ORDER BY
  customers.country
LIMIT 50;

-- [JAVA path] rawCustomers
-- Source variable: rawCustomers
SELECT
  id,
  email,
  signup_date,
  country,
  plan_type,
  mrr
FROM `axp-lumid.dw_anon.raw_customers`
WHERE
  NOT email IS NULL AND mrr > 0;

-- [LLM path] customerDeciles
-- Source: customerDeciles
SELECT
  customer_id,
  total_ltv,
  acquisition_cost,
  NTILE(10) OVER (ORDER BY total_ltv DESC) AS ltv_decile,
  SUM(total_ltv) OVER (ORDER BY total_ltv DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ltv,
  (
    total_ltv - acquisition_cost
  ) / acquisition_cost AS roi_multiplier
FROM `axp-lumid.dw_anon.customer_ltv`
WHERE
  acquisition_cost > 0
ORDER BY
  ltv_decile ASC,
  total_ltv DESC;

-- [LLM path] regionProductMatrix
-- Source: regionProductMatrix
SELECT
  regions.region_id,
  regions.region_name,
  regions.currency,
  product_catalog.product_id,
  product_catalog.product_name,
  product_catalog.base_price,
  (
    product_catalog.base_price * fx_rate
  ) AS local_price
FROM `axp-lumid.dw_anon.regions`
CROSS JOIN axp-lumid.dw_anon.product_catalog
  ON None
WHERE
  product_catalog.is_active = TRUE
ORDER BY
  regions.region_name ASC,
  product_catalog.product_name ASC;

-- [JAVA path] cleanedEvents
-- Source variable: cleanedEvents
SELECT
  length(raw_payload) AS payload_length,
  dayofweek(event_ts) AS day_of_week,
  hour(event_ts) AS hour_of_day,
  lower(TRIM(event_type)) AS normalized_event_type,
  user_id
FROM `axp-lumid.dw_anon.raw_events`
WHERE
  lower(TRIM(event_type)) IN ('click', 'purchase', 'view', 'cart_add')
  AND length(raw_payload) < 10000
GROUP BY
  user_id,
  normalized_event_type,
  hour_of_day
ORDER BY
  NULL DESC;

-- [LLM path] flattenedTags
-- Source: flattenedTags
-- Note: The explode(col('tags')) and lower(col('tag')) operations are approximated as a lateral view explode and LOWER() function. Full SQL equivalent would be: WITH exploded AS (SELECT article_id, published_at, LOWER(tag) AS tag_lower FROM articles LATERAL VIEW EXPLODE(tags) t AS tag) SELECT tag_lower, COUNT(article_id) AS article_count, MIN(published_at) AS first_used, MAX(published_at) AS last_used FROM exploded GROUP BY tag_lower HAVING COUNT(article_id) >= 5 ORDER BY article_count DESC
SELECT
  tag_lower,
  COUNT(article_id) AS article_count,
  MIN(published_at) AS first_used,
  MAX(published_at) AS last_used
FROM `axp-lumid.dw_anon.articles`
GROUP BY
  tag_lower
HAVING
  article_count >= 5
ORDER BY
  article_count DESC;

-- [JAVA path] financialModelOutput
-- Source variable: financialModelOutput
SELECT
  trader_metadata.region AS trading_region,
  DATE_DIFF(settlement_date, trade_date, DAY) AS days_to_settle,
  notional_usd,
  pnl_usd AS pnl_local,
  desk,
  asset_class,
  strategy_code,
  trader_metadata.region
FROM `axp-lumid.dw_anon.trade_ledger` AS trade_ledger
INNER JOIN `axp-lumid.dw_anon.counterparties` AS counterparties
  ON trade_ledger.counterparty_id = counterparties.id
INNER JOIN `axp-lumid.dw_anon.trader_metadata` AS trader_metadata
  ON trade_ledger.trader_id = trader_metadata.trader_id
LEFT JOIN `axp-lumid.dw_anon.desk_limits` AS desk_limits
  ON trade_ledger.desk = desk_limits.desk
  AND trade_ledger.asset_class = desk_limits.asset_class
LEFT JOIN `axp-lumid.dw_anon.fx_rates` AS fx_rates
  ON trade_ledger.currency = fx_rates.ccy
  AND trade_ledger.trade_date = fx_rates.rate_date
WHERE
  trade_date > '2024-01-01'
  AND trade_date < '2025-01-01'
  AND counterparties.credit_rating IN ('AAA', 'AA+', 'AA')
  AND is_breach = FALSE
GROUP BY
  desk,
  asset_class,
  strategy_code,
  trading_region
ORDER BY
  desk
LIMIT 100;

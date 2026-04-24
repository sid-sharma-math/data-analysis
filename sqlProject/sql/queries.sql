/* =========================================================================
   OVB Analytics — Sample Analytical Queries
   -------------------------------------------------------------------------
   Each query answers a business question and demonstrates one core SQL
   pattern. To run a single query in SSMS: highlight the block (including
   any WITH clause) and press F5. If nothing is highlighted, F5 runs the
   whole file.
   ========================================================================= */

USE OVBAnalytics;
GO


/* -------------------------------------------------------------------------
   Q1.  Business snapshot — basic aggregation
   -------------------------------------------------------------------------
   Question : "How big is our book of business?"
   Teaches  : COUNT, SUM, AVG on a single table.
   Expected : 1 row, 4 columns.
   ------------------------------------------------------------------------- */
SELECT
    COUNT(*)                 AS total_contracts,
    SUM(contract_value)      AS total_contract_value_eur,
    SUM(commission)          AS total_commission_eur,
    AVG(contract_value)      AS avg_contract_value_eur
FROM dbo.fact_contracts;
GO


/* -------------------------------------------------------------------------
   Q2.  Revenue by product category — JOIN + GROUP BY
   -------------------------------------------------------------------------
   Question : "Which product categories drive the most revenue?"
   Teaches  : The bread-and-butter pattern — join a fact to a dim, group
              by a dim attribute, aggregate the fact measures.
   Expected : 4 rows (Insurance / Pension / Investment / Mortgage).
   ------------------------------------------------------------------------- */
SELECT
    p.product_category,
    COUNT(*)                     AS contracts_signed,
    SUM(f.contract_value)        AS total_value_eur,
    SUM(f.commission)            AS total_commission_eur,
    AVG(f.contract_value)        AS avg_value_eur
FROM dbo.fact_contracts AS f
JOIN dbo.dim_product    AS p ON f.product_sk = p.product_sk
GROUP BY p.product_category
ORDER BY total_value_eur DESC;
GO


/* -------------------------------------------------------------------------
   Q3.  Top 10 advisers by commission — CTE + window function (RANK)
   -------------------------------------------------------------------------
   Question : "Who are our top performers?"
   Teaches  :
     - CTE (WITH clause) for readability: compute once, reference below.
     - RANK() OVER (ORDER BY ...) assigns 1, 2, 3... to rows ordered by a
       measure. Ties get the same rank (skipping the next value).
     - T-SQL's TOP N (the MS-SQL equivalent of LIMIT).
   Expected : 10 rows, sorted highest-commission first.
   ------------------------------------------------------------------------- */
WITH adviser_commission AS (
    SELECT
        a.adviser_id,
        CONCAT(a.first_name, ' ', a.last_name)  AS adviser_name,
        a.region,
        a.experience_level,
        SUM(f.commission)                       AS total_commission
    FROM dbo.fact_contracts AS f
    JOIN dbo.dim_adviser    AS a ON f.adviser_sk = a.adviser_sk
    GROUP BY a.adviser_id, a.first_name, a.last_name, a.region, a.experience_level
)
SELECT TOP 10
    RANK() OVER (ORDER BY total_commission DESC) AS commission_rank,
    adviser_name,
    region,
    experience_level,
    total_commission
FROM adviser_commission
ORDER BY total_commission DESC;
GO


/* -------------------------------------------------------------------------
   Q4.  Monthly contract value with year-to-date running total
   -------------------------------------------------------------------------
   Question : "How does month-by-month signed value trend within a year?"
   Teaches  :
     - SUM() OVER (PARTITION BY ... ORDER BY ...): a "window function" that
       computes a running total. PARTITION BY resets the running total at
       the start of each year; ORDER BY defines the accumulation sequence.
     - Wrapping an aggregation inside a subquery to compute something
       across those aggregates.
   Expected : one row per (year, month) with monthly and YTD totals.
   ------------------------------------------------------------------------- */
SELECT
    year_num,
    month_num,
    month_name,
    monthly_total,
    SUM(monthly_total) OVER (
        PARTITION BY year_num
        ORDER BY month_num
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS ytd_running_total
FROM (
    SELECT
        d.year_num,
        d.month_num,
        MAX(d.month_name)            AS month_name,
        SUM(f.contract_value)        AS monthly_total
    FROM dbo.fact_contracts AS f
    JOIN dbo.dim_date       AS d ON f.signing_date_sk = d.date_sk
    GROUP BY d.year_num, d.month_num
) AS monthly
ORDER BY year_num, month_num;
GO


/* -------------------------------------------------------------------------
   Q5.  Client age buckets — CASE WHEN + date arithmetic
   -------------------------------------------------------------------------
   Question : "How are our clients distributed across age groups, and
               how many contracts have they signed?"
   Teaches  :
     - CASE WHEN to derive a categorical column from a numeric one.
     - DATEDIFF(YEAR, ...) for approximate age (T-SQL idiom).
     - LEFT JOIN so clients with zero contracts still appear.
     - Grouping by the same CASE expression used in SELECT.
   Expected : 4 rows (one per age bucket).
   ------------------------------------------------------------------------- */
WITH client_age AS (
    SELECT
        c.client_sk,
        CASE
            WHEN DATEDIFF(YEAR, c.date_of_birth, GETDATE()) < 30 THEN '< 30'
            WHEN DATEDIFF(YEAR, c.date_of_birth, GETDATE()) < 45 THEN '30-44'
            WHEN DATEDIFF(YEAR, c.date_of_birth, GETDATE()) < 60 THEN '45-59'
            ELSE                                                      '60+'
        END AS age_group
    FROM dbo.dim_client AS c
)
SELECT
    ca.age_group,
    COUNT(DISTINCT ca.client_sk)    AS clients,
    COUNT(f.contract_sk)            AS contracts_signed,
    SUM(f.contract_value)           AS total_value_eur
FROM client_age             AS ca
LEFT JOIN dbo.fact_contracts AS f ON f.client_sk = ca.client_sk
GROUP BY ca.age_group
ORDER BY
    CASE ca.age_group
        WHEN '< 30'  THEN 1
        WHEN '30-44' THEN 2
        WHEN '45-59' THEN 3
        WHEN '60+'   THEN 4
    END;
GO


/* -------------------------------------------------------------------------
   Q6.  Quarter-over-quarter revenue change — LAG()
   -------------------------------------------------------------------------
   Question : "Is each quarter growing vs the previous one?"
   Teaches  :
     - LAG(col) OVER (ORDER BY ...) returns the value from the PREVIOUS
       row. Essential for time-series "% change" metrics.
     - CAST + NULLIF for safe percentage division (avoid divide-by-zero).
   Expected : one row per (year, quarter), with previous-quarter value
              and QoQ % change.
   ------------------------------------------------------------------------- */
WITH quarterly AS (
    SELECT
        d.year_num,
        d.quarter_num,
        SUM(f.contract_value) AS quarter_total
    FROM dbo.fact_contracts AS f
    JOIN dbo.dim_date       AS d ON f.signing_date_sk = d.date_sk
    GROUP BY d.year_num, d.quarter_num
)
SELECT
    year_num,
    quarter_num,
    quarter_total,
    LAG(quarter_total) OVER (ORDER BY year_num, quarter_num)          AS prev_quarter_total,
    quarter_total - LAG(quarter_total) OVER (ORDER BY year_num, quarter_num) AS qoq_delta,
    CAST(
        (quarter_total - LAG(quarter_total) OVER (ORDER BY year_num, quarter_num))
        * 100.0
        / NULLIF(LAG(quarter_total) OVER (ORDER BY year_num, quarter_num), 0)
        AS DECIMAL(6, 2)
    ) AS qoq_pct_change
FROM quarterly
ORDER BY year_num, quarter_num;
GO


/* -------------------------------------------------------------------------
   Q7.  Status mix per product category — conditional aggregation (pivot)
   -------------------------------------------------------------------------
   Question : "Of the contracts in each category, how many are Active vs
               Cancelled vs Completed?"
   Teaches  :
     - Conditional aggregation: SUM(CASE WHEN ...) acts like a pivot,
       turning distinct values of one column into multiple output columns.
     - A single scan of the fact table produces all three counts.
   Expected : 4 rows (one per category), 5 columns.
   ------------------------------------------------------------------------- */
SELECT
    p.product_category,
    COUNT(*)                                                          AS total_contracts,
    SUM(CASE WHEN f.status = 'Active'    THEN 1 ELSE 0 END)           AS active_cnt,
    SUM(CASE WHEN f.status = 'Cancelled' THEN 1 ELSE 0 END)           AS cancelled_cnt,
    SUM(CASE WHEN f.status = 'Completed' THEN 1 ELSE 0 END)           AS completed_cnt,
    CAST(
        SUM(CASE WHEN f.status = 'Cancelled' THEN 1.0 ELSE 0 END)
        / NULLIF(COUNT(*), 0) * 100
        AS DECIMAL(5, 2)
    )                                                                 AS cancellation_rate_pct
FROM dbo.fact_contracts AS f
JOIN dbo.dim_product    AS p ON f.product_sk = p.product_sk
GROUP BY p.product_category
ORDER BY cancellation_rate_pct DESC;
GO


/* -------------------------------------------------------------------------
   Q8.  Weekday vs weekend signings — leveraging dim_date pre-computed flags
   -------------------------------------------------------------------------
   Question : "Do weekends actually see material contract volume?"
   Teaches  :
     - Why pre-computed date attributes (is_weekend) are worth storing:
       every report that needs weekend/weekday logic just uses the flag,
       no repeated date-function reasoning.
     - Multi-table JOIN (fact + product + date).
   Expected : 4 rows, comparing weekday vs weekend revenue per category.
   ------------------------------------------------------------------------- */
SELECT
    p.product_category,
    SUM(CASE WHEN d.is_weekend = 0 THEN f.contract_value ELSE 0 END) AS weekday_revenue_eur,
    SUM(CASE WHEN d.is_weekend = 1 THEN f.contract_value ELSE 0 END) AS weekend_revenue_eur,
    CAST(
        SUM(CASE WHEN d.is_weekend = 1 THEN f.contract_value ELSE 0 END)
        * 100.0
        / NULLIF(SUM(f.contract_value), 0)
        AS DECIMAL(5, 2)
    ) AS weekend_share_pct
FROM dbo.fact_contracts AS f
JOIN dbo.dim_product    AS p ON f.product_sk = p.product_sk
JOIN dbo.dim_date       AS d ON f.signing_date_sk = d.date_sk
GROUP BY p.product_category
ORDER BY weekend_share_pct DESC;
GO


/* -------------------------------------------------------------------------
   Q9.  Advisers with zero cancellations — EXISTS / NOT EXISTS
   -------------------------------------------------------------------------
   Question : "Which advisers have signed real business AND never had a
               contract cancelled?"
   Teaches  :
     - EXISTS / NOT EXISTS subqueries — often faster and clearer than
       IN / NOT IN for "is there any matching row?" checks.
     - Chaining two conditions: active book AND clean record.
   Expected : subset of advisers, sorted alphabetically.
   ------------------------------------------------------------------------- */
SELECT
    a.adviser_id,
    CONCAT(a.first_name, ' ', a.last_name) AS adviser_name,
    a.region,
    a.experience_level
FROM dbo.dim_adviser AS a
WHERE EXISTS (
        SELECT 1
        FROM dbo.fact_contracts AS f
        WHERE f.adviser_sk = a.adviser_sk
      )
  AND NOT EXISTS (
        SELECT 1
        FROM dbo.fact_contracts AS f
        WHERE f.adviser_sk = a.adviser_sk
          AND f.status     = 'Cancelled'
      )
ORDER BY a.last_name, a.first_name;
GO


/* -------------------------------------------------------------------------
   Q10. Top 3 products per region — ROW_NUMBER() PARTITION BY
   -------------------------------------------------------------------------
   Question : "In each region, what are the three highest-grossing
               products?"
   Teaches  :
     - ROW_NUMBER() OVER (PARTITION BY X ORDER BY Y): assigns 1,2,3...
       within each X group, ordered by Y. The canonical "top N per group"
       trick.
     - Filter on the row number in an outer query (you cannot filter on
       a window-function result in the same level's WHERE — hence the CTE).
     - Multi-column, multi-table aggregation feeding a window function.
   Expected : 3 rows per region × N regions.
   ------------------------------------------------------------------------- */
WITH regional_product_revenue AS (
    SELECT
        a.region,
        p.product_name,
        p.product_category,
        SUM(f.contract_value) AS revenue_eur,
        ROW_NUMBER() OVER (
            PARTITION BY a.region
            ORDER BY SUM(f.contract_value) DESC
        ) AS rank_in_region
    FROM dbo.fact_contracts AS f
    JOIN dbo.dim_adviser    AS a ON f.adviser_sk = a.adviser_sk
    JOIN dbo.dim_product    AS p ON f.product_sk = p.product_sk
    GROUP BY a.region, p.product_name, p.product_category
)
SELECT
    region,
    rank_in_region,
    product_name,
    product_category,
    revenue_eur
FROM regional_product_revenue
WHERE rank_in_region <= 3
ORDER BY region, rank_in_region;
GO

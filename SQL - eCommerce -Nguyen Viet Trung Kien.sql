---Query 01: calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)---
SELECT 
    FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) AS month
    ,SUM(totals.visits) AS visits
    ,SUM(totals.pageviews) AS pageviews
    ,SUM(totals.transactions) AS transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix BETWEEN '0101' AND '0331'
GROUP BY month
ORDER BY month;
---Query 02: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)---
WITH total_source AS 
(
  SELECT 
      trafficSource.source AS source
      ,SUM(totals.visits) AS total_visits
      ,SUM(totals.bounces) AS total_no_of_bounces
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
  GROUP BY trafficSource.source
)
    SELECT
        source
        ,total_visits
        ,total_no_of_bounces
        ,100* total_no_of_bounces / total_visits AS bounce_rate
    FROM total_source
    ORDER BY total_visits DESC;
---Query 3: Revenue by traffic source by week, by month in June 2017---
WITH month_revenue AS 
(
  SELECT 
      'Month' AS time_type 
      ,FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', date)) AS time
      ,trafficSource.source AS source
      ,SUM(product.productRevenue) / 1000000 AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
      ,UNNEST (hits) hits
      ,UNNEST (hits.product) product
  WHERE product.productRevenue IS NOT NULL
  GROUP BY time,source
),
week_revenue AS
(
  SELECT 
      'Week' AS time_type 
      ,FORMAT_DATE('%Y%W',PARSE_DATE('%Y%m%d', date)) AS time
      ,trafficSource.source AS source
      ,SUM(product.productRevenue) / 1000000 AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
      ,UNNEST (hits) hits
      ,UNNEST (hits.product) product
  WHERE product.productRevenue IS NOT NULL
  GROUP BY time,source   
)
    SELECT * 
    FROM month_revenue
    UNION ALL
    SELECT *
    FROM week_revenue
    ORDER BY revenue DESC;
---Query 04: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.---
WITH purchaser AS 
(
  SELECT 
      FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month
      ,SUM(totals.pageviews) AS total_pageview
      ,COUNT(DISTINCT(fullVisitorId)) AS number_unique_user
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      ,UNNEST (hits) hits
      ,UNNEST (hits.product) product
  WHERE _table_suffix BETWEEN '0601' AND '0731'
      AND totals.transactions >=1
      AND product.productRevenue IS NOT NULL
  GROUP BY month
),
non_purchaser AS 
(
  SELECT 
      FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month
      ,SUM(totals.pageviews) AS total_pageview_non_purchase
      ,COUNT(DISTINCT(fullVisitorId)) AS number_unique_user_non_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      ,UNNEST (hits) hits
      ,UNNEST (hits.product) product
  WHERE _table_suffix BETWEEN '0601' AND '0731'
      AND totals.transactions IS NULL
      AND product.productRevenue IS NULL
  GROUP BY month
)
    SELECT
        purchaser.month
        ,purchaser.total_pageview / purchaser.number_unique_user AS avg_pageviews_purchase
        ,non_purchaser.total_pageview_non_purchase / non_purchaser.number_unique_user_non_purchase AS avg_pageviews_non_purchase
    FROM purchaser 
    LEFT JOIN non_purchaser 
        ON purchaser.month = non_purchaser.month
    ORDER BY purchaser.month;
---Query 05: Average number of transactions per user that made a purchase in July 2017---
WITH purchaser AS 
(
  SELECT 
      FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month
      ,SUM(totals.transactions) AS total_transactions
      ,COUNT(DISTINCT(fullVisitorId)) AS number_unique_user
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
      ,UNNEST (hits) hits
      ,UNNEST (hits.product) product
  WHERE totals.transactions >=1
      AND product.productRevenue IS NOT NULL
  GROUP BY month
)
    SELECT
        month
        ,total_transactions / number_unique_user AS Avg_total_transactions_per_user
    FROM purchaser 
    ORDER BY month;
---Query 06: Average amount of money spent per session. Only include purchaser data in July 2017---
WITH purchaser AS 
(
  SELECT 
      FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month
      ,SUM(product.productRevenue) AS total_revenue
      ,SUM(totals.visits) AS  total_visit
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
      ,UNNEST (hits) hits
      ,UNNEST (hits.product) product
  WHERE totals.transactions IS NOT NULL
      AND product.productRevenue IS NOT NULL
    GROUP BY month
)
    SELECT
        month
        ,ROUND((total_revenue/total_visit) / 1000000, 3) AS avg_revenue_by_user_per_visit
    FROM purchaser 
    ORDER BY month;
---Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.---
WITH customer AS 
(
  SELECT 
      DISTINCT(fullVisitorId)
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
      ,UNNEST (hits) hits
      ,UNNEST (hits.product) product
  WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
      AND hits.eCommerceAction.action_type = '6'
      AND totals.transactions >= 1
      AND product.productRevenue IS NOT NULL 
)
    SELECT 
        product.v2ProductName AS other_purchased_products
        ,SUM(product.productQuantity) AS quantity
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` sessions
        ,UNNEST (sessions.hits) hits
        ,UNNEST (hits.product) product
    INNER JOIN customer
        ON sessions.fullVisitorId = customer.fullVisitorId
    WHERE product.v2ProductName != "YouTube Men's Vintage Henley"
        AND hits.eCommerceAction.action_type = '6'
        AND sessions.totals.transactions >= 1
        AND product.productRevenue IS NOT NULL
    GROUP BY other_purchased_products
    ORDER BY quantity DESC;
---"Query 08: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase.Add_to_cart_rate = number product  add to cart/number product view. Purchase_rate = number product purchase/number product view. The output should be calculated in product level."---
WITH product_view AS 
(
  SELECT 
      FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) AS month
      ,COUNT(*) AS num_product_view
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      ,UNNEST (hits) hits
      ,UNNEST (hits.product) product
  WHERE _table_suffix BETWEEN '0101' AND '0331'
      AND hits.eCommerceAction.action_type = '2'
  GROUP BY month
),
add_to_cart AS 
(
  SELECT 
      FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) AS month
      ,COUNT(*) AS num_add_to_cart
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      ,UNNEST (hits) hits
      ,UNNEST (hits.product) product
  WHERE _table_suffix BETWEEN '0101' AND '0331'
      AND hits.eCommerceAction.action_type = '3'
  GROUP BY month
),
purchase AS 
(
  SELECT 
      FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) AS month
      ,COUNT(*) AS  num_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      ,UNNEST (hits) hits
      ,UNNEST (hits.product) product
  WHERE _table_suffix BETWEEN '0101' AND '0331'
      AND hits.eCommerceAction.action_type = '6'
      AND product.productRevenue IS NOT NULL
  GROUP BY month
)
    SELECT 
        product_view.month
        ,num_product_view
        ,num_add_to_cart
        ,num_purchase
        ,100* num_add_to_cart / num_product_view AS add_to_cart_rate
        ,100* num_purchase / num_product_view AS purchase_rate
    FROM product_view
    INNER JOIN add_to_cart
    ON product_view.month = add_to_cart.month
    INNER JOIN purchase
    ON product_view.month = purchase.month
    ORDER BY product_view.month


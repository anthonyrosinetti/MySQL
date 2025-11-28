WITH new_customers AS (
  SELECT DISTINCT
    id_customer,
    DATE_TRUNC(date,MONTH) AS first_purchase_month
  FROM
    `cdp-boryl.aov_analysis.enriched_orders`
  WHERE
    new_customer
)

SELECT
  DATE_DIFF(DATE_TRUNC(o.date,MONTH),first_purchase_month,MONTH)+1 AS order_month_rank,
  DATE_TRUNC(o.date,MONTH) AS order_month,
  o.order_rank,
  o.id_order,
  o.customer_type,
  o.items_count,
  o.average_item_price,
  ca_ht,
  promotion_rate,
  first_purchase_month
FROM
  `cdp-boryl.aov_analysis.enriched_orders` o
  LEFT JOIN new_customers nc
  USING (id_customer)
WHERE
  nc.first_purchase_month IS NOT NULL
  AND
  customer_type = 'B2C'

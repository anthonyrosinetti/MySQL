CREATE OR REPLACE TABLE `cdp-boryl.ltv_analysis.ltv_groups` AS (
  WITH l1 AS (
    SELECT
      id_customer,
      SUM(ca_ht) AS cltv,
      COUNT(DISTINCT id_order) AS orders_total,
      AVG(ca_ht) AS aov
    FROM
      `cdp-boryl.aov_analysis.enriched_orders`
    WHERE
      # exclude refunded orders
      current_state NOT IN (7,34)
      AND
      # keep B2C customers only
      customer_type = 'B2C'
      AND
      # keep clients only
      id_default_group = 3
    GROUP BY
      id_customer
    ORDER BY
      2 DESC
  )

  SELECT
    *,
    NTILE(4) OVER (ORDER BY cltv ASC) AS cltv_group
  FROM
    l1
  WHERE
    cltv > 0
)

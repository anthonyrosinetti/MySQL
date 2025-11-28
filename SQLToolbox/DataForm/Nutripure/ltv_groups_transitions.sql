CREATE OR REPLACE TABLE `cdp-boryl.ltv_analysis.ltv_groups_transitions` AS (
  WITH layer_1 AS (
    SELECT
      id_customer,
      date,
      -- first_order_date,
      -- id_order,
      order_rank,
      seniority,
      age_range,
      gender,
      with_discount,
      free_shipping,
      persona,
      main_type,
      items_count,
      average_item_price,
      -- ca_ht,
      SUM(ca_ht) OVER (PARTITION BY id_customer ORDER BY order_rank ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulated_value
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
    ORDER BY
      1, cumulated_value ASC
  ),

  layer_2 AS (
    SELECT
      *,
      LAG(cumulated_value) OVER (PARTITION BY id_customer ORDER BY order_rank ASC) AS previous_cumulated_value
    FROM
      layer_1
  )

  SELECT
    *,
    CASE
      WHEN
        cumulated_value > 239 AND previous_cumulated_value < 239
        -- OR
        -- cumulated_value > 239 AND previous_cumulated_value IS NULL
      THEN 'to_vip_switch'
      WHEN
        cumulated_value > 104 AND previous_cumulated_value < 104
        -- OR
        -- cumulated_value > 104 AND previous_cumulated_value IS NULL
      THEN 'to_3_switch'
      WHEN
        cumulated_value > 51 AND previous_cumulated_value < 51
        -- OR
        -- cumulated_value > 51 AND previous_cumulated_value IS NULL
      THEN 'to_2_switch'
      ELSE NULL
    END AS group_switch_type
  FROM
    layer_2
  ORDER BY
    id_customer, cumulated_value ASC
)

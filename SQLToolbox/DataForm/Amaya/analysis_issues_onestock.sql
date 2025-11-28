
-- Config block
config {
  type: "table",
  bigquery: {
    partitionBy: "date"
  },  
  schema: "onestock",
  tags: ["intraday_run", "daily_run"]
}

WITH stores AS (
  SELECT DISTINCT
    e.id AS store_id,
    e.name AS store_name,
    regional_area
  FROM
    ${ref("raw_onestock_endpoints")} e
    LEFT JOIN
      ${ref("stores_regional_areas")} r
    USING
      (id)
),

item_id_skus_names AS (
  SELECT DISTINCT
    JSON_EXTRACT_SCALAR(oi.information,'$.gtin') AS item_sku,
    CAST(oi.item_id AS STRING) AS item_id
  FROM
    ${ref("raw_onestock_orders")}  o,UNNEST(order_items) oi
),

ckc_long_items_parcels_scope AS (
  SELECT DISTINCT
    o.id AS order_id,
    MAX(CASE WHEN lig.state = 'claimed_warehouse' THEN True ELSE False END) AS claimed_warehouse,
    MAX(CASE WHEN p.state = 'dispatched' THEN True ELSE False END) AS dispatched
  FROM
    ${ref("raw_onestock_orders")}  o,UNNEST(line_item_groups) lig,UNNEST(parcels) p
  WHERE
    o.delivery.type = 'ckclong_ckclong'
  GROUP BY
    order_id
),

orders_taxes_amount AS (
  SELECT DISTINCT
    o.id AS order_id,
    SUM(t.amount) AS order_taxes
  FROM
    ${ref("raw_onestock_orders")}  o,UNNEST(order_items) oi,UNNEST(oi.pricing_details.taxes) t
  GROUP BY
    order_id
),

orders_refusal_reasons AS (
  SELECT DISTINCT
    o.id AS order_id,
    lig.reason,
    COUNT(DISTINCT lig.id) AS occurrences
  FROM
    ${ref("raw_onestock_orders")}  o,UNNEST(line_item_groups) lig
  WHERE
    o.state = 'removed'
  GROUP BY
    order_id,
    reason
),

orders_main_refusal_reasons AS (
  SELECT DISTINCT
    order_id,
    CASE WHEN occurrences = MAX(occurrences) OVER (PARTITION BY order_id) THEN reason ELSE NULL END AS refusal_reason
  FROM
    orders_refusal_reasons
),

products_pricing_amount AS (
  SELECT DISTINCT
    JSON_EXTRACT_SCALAR(oi.information,'$.gtin') AS item_sku,
    o.id AS order_id,
    MAX(o.pricing_details.price) OVER (PARTITION BY o.id) AS order_price,
    SUM(oi.pricing_details.price) OVER (PARTITION BY o.id, JSON_EXTRACT_SCALAR(oi.information,'$.gtin')) AS product_price,
    SUM(oi.pricing_details.price) OVER (PARTITION BY o.id) AS total_product_price,
  FROM
    ${ref("raw_onestock_orders")}  o,UNNEST(order_items) oi
),

layer2_products_pricing_amount AS (
  SELECT DISTINCT
    item_sku,
    order_id,
    product_price-SAFE_DIVIDE(product_price,total_product_price)*(total_product_price-order_price) AS adjusted_product_price
  FROM
    products_pricing_amount      
),  

products_taxes_amount AS (
  SELECT DISTINCT
    JSON_EXTRACT_SCALAR(oi.information,'$.gtin') AS item_sku,
    o.id AS order_id,    
    SUM(t.amount) AS product_taxes
  FROM
    ${ref("raw_onestock_orders")}  o,UNNEST(order_items) oi,UNNEST(oi.pricing_details.taxes) t
  GROUP BY
    item_sku,
    order_id
),

preanalysed_disposition_orders AS (
  SELECT DISTINCT
    o.id,
    d.disposition
  FROM
    ${ref("raw_onestock_orders")} o,UNNEST(line_item_groups) lig
    LEFT JOIN ${ref("raw_onestock_stock_dispositions")} d
    ON
      DATE(created_at_timestamp, 'Europe/Paris') = DATE(TIMESTAMP_SECONDS(o.date),'Europe/Paris')
      AND
      d.item_id = lig.item_id
      AND
      d.endpoint_id = o.delivery.destination.endpoint_id
  WHERE
    disposition IS NOT NULL
),

preanalysed_refusals_order_scope AS (
  SELECT
    increment_id AS order_id,
    DATE(DATETIME(created_at,'Europe/Paris')) AS date,
    DATETIME(created_at,'Europe/Paris') AS datetime,
    CAST(NULL AS STRING) AS delivery_type,
    ROUND(SUM(base_grand_total)-SUM(base_tax_amount)-SUM(base_tax_refunded),2) AS ht_revenues,
    ROUND(SUM(base_grand_total),2) AS ttc_revenues,
    CAST(NULL AS STRING) AS item_sku,    
    DATETIME(updated_at,'Europe/Paris') AS order_last_update_datetime,
    status AS state,
    CAST(NULL AS STRING) AS refusal_reason,
    'W0001' AS store_id,
    CAST(NULL AS STRING) AS return_store_name
  FROM
    ${ref("raw_magento_orders")} mo
    LEFT JOIN
      ${ref("raw_onestock_orders")} o
      ON
        o.id = mo.increment_id      
  WHERE
    (
      (
        o.delivery.type IN ('ckcexpress_ckcexpress','ckclong_ckclong')
      )
      OR
      (
        o.delivery.type NOT IN ('ckcexpress_ckcexpress','ckclong_ckclong')
        AND
        mo.status IN ('preparation_in_progress','ready_to_send','complete')
      )
    )
    AND
    o.state = 'fulfilled'
    AND
    EXTRACT(YEAR FROM TIMESTAMP_TRUNC(mo.created_at,YEAR)) >= 2025      
  GROUP BY
    order_id,
    date,
    datetime,
    store_id,
    delivery_type,
    item_sku,
    order_last_update_datetime,
    state,
    refusal_reason

  UNION ALL

  SELECT
    o.id AS order_id,
    DATE(DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris')) AS date,
    DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris') AS datetime,
    'ckc_long' AS delivery_type,
    CASE WHEN ROUND(SUM(pricing_details.price)-SUM(t.order_taxes),2) < 0 THEN SUM(pricing_details.price) ELSE ROUND(SUM(pricing_details.price)-SUM(t.order_taxes),2) END AS ht_revenues,
    ROUND(SUM(pricing_details.price),2) AS ttc_revenues,      
    CAST(NULL AS STRING) AS item_sku,
    DATETIME(TIMESTAMP_SECONDS(last_update),'Europe/Paris') AS order_last_update_datetime,
    o.state,
    d.disposition AS refusal_reason,
    delivery.destination.endpoint_id AS store_id,
    CAST(NULL AS STRING) AS return_store_name
  FROM
    ${ref("raw_onestock_orders")}  o
    INNER JOIN preanalysed_disposition_orders d ON d.id = o.id
    LEFT JOIN orders_taxes_amount t ON t.order_id = o.id
  WHERE
    delivery.type = 'ckclong_ckclong'
  GROUP BY
    order_id,
    date,
    datetime,
    store_id,
    delivery_type,
    item_sku,
    order_last_update_datetime,
    state,
    refusal_reason

  UNION ALL

  SELECT
    id AS order_id,
    DATE(DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris')) AS date,
    DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris') AS datetime,
    'ckc_long' AS delivery_type,
    CASE WHEN ROUND(SUM(pricing_details.price)-SUM(t.order_taxes),2) < 0 THEN SUM(pricing_details.price) ELSE ROUND(SUM(pricing_details.price)-SUM(t.order_taxes),2) END AS ht_revenues,
    ROUND(SUM(pricing_details.price),2) AS ttc_revenues,      
    CAST(NULL AS STRING) AS item_sku,
    DATETIME(TIMESTAMP_SECONDS(last_update),'Europe/Paris') AS order_last_update_datetime,
    'global' AS state,
    rr.refusal_reason,
    delivery.destination.endpoint_id AS store_id,
    CAST(NULL AS STRING) AS return_store_name
  FROM
    ${ref("raw_onestock_orders")}  o
    LEFT JOIN orders_taxes_amount t ON t.order_id = o.id
    LEFT JOIN orders_main_refusal_reasons rr USING (order_id)
  WHERE
    delivery.type = 'ckclong_ckclong'
  GROUP BY
    order_id,
    date,
    datetime,
    store_id,
    delivery_type,
    item_sku,
    order_last_update_datetime,
    state,
    refusal_reason

  UNION ALL

  SELECT
    o.id AS order_id,
    DATE(DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris')) AS date,
    DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris') AS datetime,
    'ckc_express' AS delivery_type,
    CASE WHEN ROUND(SUM(pricing_details.price)-SUM(t.order_taxes),2) < 0 THEN SUM(pricing_details.price) ELSE ROUND(SUM(pricing_details.price)-SUM(t.order_taxes),2) END AS ht_revenues,
    ROUND(SUM(pricing_details.price),2) AS ttc_revenues,
    CAST(NULL AS STRING) AS item_sku,
    DATETIME(TIMESTAMP_SECONDS(last_update),'Europe/Paris') AS order_last_update_datetime,
    o.state,
    d.disposition AS refusal_reason,
    delivery.destination.endpoint_id AS store_id,
    CAST(NULL AS STRING) AS return_store_name
  FROM
    ${ref("raw_onestock_orders")}  o
    INNER JOIN preanalysed_disposition_orders d ON d.id = o.id
    LEFT JOIN orders_taxes_amount t ON t.order_id = o.id
  WHERE
    delivery.type = 'ckcexpress_ckcexpress'
  GROUP BY
    order_id,
    date,
    datetime,
    store_id,
    delivery_type,
    item_sku,
    order_last_update_datetime,
    state,
    refusal_reason

  UNION ALL

  SELECT
    id AS order_id,
    DATE(DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris')) AS date,
    DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris') AS datetime,
    'ckc_express' AS delivery_type,
    CASE WHEN ROUND(SUM(pricing_details.price)-SUM(t.order_taxes),2) < 0 THEN SUM(pricing_details.price) ELSE ROUND(SUM(pricing_details.price)-SUM(t.order_taxes),2) END AS ht_revenues,
    ROUND(SUM(pricing_details.price),2) AS ttc_revenues,
    CAST(NULL AS STRING) AS item_sku,
    DATETIME(TIMESTAMP_SECONDS(last_update),'Europe/Paris') AS order_last_update_datetime,
    'global' AS state,
    rr.refusal_reason,
    delivery.destination.endpoint_id AS store_id,
    CAST(NULL AS STRING) AS return_store_name
  FROM
    ${ref("raw_onestock_orders")}  o
    LEFT JOIN orders_taxes_amount t ON t.order_id = o.id
    LEFT JOIN orders_main_refusal_reasons rr USING (order_id)
  WHERE
    delivery.type = 'ckcexpress_ckcexpress'
  GROUP BY
    order_id,
    date,
    datetime,
    store_id,
    delivery_type,
    item_sku,
    order_last_update_datetime,
    state,
    refusal_reason
),

preanalysed_refusals_product_scope AS (
  SELECT
    o.id AS order_id,
    DATE(DATETIME(TIMESTAMP_SECONDS(o.date),'Europe/Paris')) AS date,
    DATETIME(TIMESTAMP_SECONDS(o.date),'Europe/Paris') AS datetime,
    'ckc_long' AS delivery_type,
    CASE WHEN ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) < 0 THEN ROUND(SUM(adjusted_product_price),2) ELSE ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) END AS ht_revenues,
    ROUND(SUM(adjusted_product_price),2) AS ttc_revenues,
    i.item_sku,
    DATETIME(TIMESTAMP_SECONDS(o.last_update),'Europe/Paris') AS order_last_update_datetime,
    lig.state,
    lig.reason AS refusal_reason,
    delivery.destination.endpoint_id AS store_id,
    CAST(NULL AS STRING) AS return_store_name
  FROM
    ${ref("raw_onestock_orders")} o,UNNEST(line_item_groups) lig
    LEFT JOIN item_id_skus_names i USING (item_id)
    LEFT JOIN layer2_products_pricing_amount pa ON pa.item_sku = i.item_sku AND pa.order_id = o.id
    LEFT JOIN products_taxes_amount pt ON pt.item_sku = i.item_sku AND pt.order_id = o.id
  WHERE
    delivery.type = 'ckclong_ckclong'
    AND
    lig.state = 'issue'
  GROUP BY
    order_id,
    date,
    datetime,
    store_id,
    delivery_type,
    item_sku,
    order_last_update_datetime,
    state,
    refusal_reason

  UNION ALL

  SELECT
    o.id AS order_id,
    DATE(DATETIME(TIMESTAMP_SECONDS(o.date),'Europe/Paris')) AS date,
    DATETIME(TIMESTAMP_SECONDS(o.date),'Europe/Paris') AS datetime,
    'ckc_express' AS delivery_type,
    CASE WHEN ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) < 0 THEN ROUND(SUM(adjusted_product_price),2) ELSE ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) END AS ht_revenues,
    ROUND(SUM(adjusted_product_price),2) AS ttc_revenues,
    i.item_sku,   
    DATETIME(TIMESTAMP_SECONDS(o.last_update),'Europe/Paris') AS order_last_update_datetime,
    lig.state,
    lig.reason AS refusal_reason,
    delivery.destination.endpoint_id AS store_id,
    CAST(NULL AS STRING) AS return_store_name
  FROM
    ${ref("raw_onestock_orders")}  o,UNNEST(line_item_groups) lig
    LEFT JOIN item_id_skus_names i USING (item_id)  
    LEFT JOIN layer2_products_pricing_amount pa ON pa.item_sku = i.item_sku AND pa.order_id = o.id
    LEFT JOIN products_taxes_amount pt ON pt.item_sku = i.item_sku AND pt.order_id = o.id
  WHERE
    delivery.type = 'ckcexpress_ckcexpress'
    AND
    lig.state = 'issue'
  GROUP BY
    order_id,
    date,
    datetime,
    store_id,
    delivery_type,
    item_sku,
    order_last_update_datetime,
    state,
    refusal_reason
),

preanalysed_no_shows_product_scope AS (
  SELECT
    o.id AS order_id,
    DATE(DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris')) AS date,
    DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris') AS datetime,
    'ckc_long' AS delivery_type,
    CASE WHEN ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) < 0 THEN ROUND(SUM(adjusted_product_price),2) ELSE ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) END AS ht_revenues,
    ROUND(SUM(adjusted_product_price),2) AS ttc_revenues,
    i.item_sku,
    DATETIME(TIMESTAMP_SECONDS(o.last_update),'Europe/Paris') AS order_last_update_datetime,
    lig.state,
    lig.reason AS refusal_reason,
    delivery.destination.endpoint_id AS store_id,
    CAST(NULL AS STRING) AS return_store_name
  FROM
    ${ref("raw_onestock_orders")}  o,UNNEST(line_item_groups) lig
    LEFT JOIN item_id_skus_names i USING (item_id)
    LEFT JOIN layer2_products_pricing_amount pa ON pa.item_sku = i.item_sku AND pa.order_id = o.id
    LEFT JOIN products_taxes_amount pt ON pt.item_sku = i.item_sku AND pt.order_id = o.id
  WHERE
    delivery.type = 'ckclong_ckclong'
    AND
    lig.reason = 'no_show'
  GROUP BY
    order_id,
    date,
    datetime,
    store_id,
    delivery_type,
    item_sku,
    order_last_update_datetime,
    state,
    refusal_reason

  UNION ALL

  SELECT
    o.id AS order_id,
    DATE(DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris')) AS date,
    DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris') AS datetime,
    'ckc_express' AS delivery_type,
    CASE WHEN ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) < 0 THEN ROUND(SUM(adjusted_product_price),2) ELSE ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) END AS ht_revenues,
    ROUND(SUM(adjusted_product_price),2) AS ttc_revenues,
    i.item_sku,   
    DATETIME(TIMESTAMP_SECONDS(o.last_update),'Europe/Paris') AS order_last_update_datetime,
    lig.state,
    lig.reason AS refusal_reason,
    delivery.destination.endpoint_id AS store_id,
    CAST(NULL AS STRING) AS return_store_name
  FROM
    ${ref("raw_onestock_orders")}  o,UNNEST(line_item_groups) lig
    LEFT JOIN item_id_skus_names i USING (item_id)
    LEFT JOIN layer2_products_pricing_amount pa ON pa.item_sku = i.item_sku AND pa.order_id = o.id
    LEFT JOIN products_taxes_amount pt ON pt.item_sku = i.item_sku AND pt.order_id = o.id
  WHERE
    delivery.type = 'ckcexpress_ckcexpress'
    AND
    lig.reason = 'no_show'    
  GROUP BY
    order_id,
    date,
    datetime,
    store_id,
    delivery_type,
    item_sku,
    order_last_update_datetime,
    state,
    refusal_reason

  UNION ALL

  SELECT
    o.id AS order_id,
    DATE(DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris')) AS date,
    DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris') AS datetime,
    'ckc_long' AS delivery_type,
    CASE WHEN ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) < 0 THEN ROUND(SUM(adjusted_product_price),2) ELSE ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) END AS ht_revenues,
    ROUND(SUM(adjusted_product_price),2) AS ttc_revenues,
    i.item_sku,
    DATETIME(TIMESTAMP_SECONDS(o.last_update),'Europe/Paris') AS order_last_update_datetime,
    lig.state,
    mo.status AS refusal_reason,
    delivery.destination.endpoint_id AS store_id,
    CAST(NULL AS STRING) AS return_store_name
  FROM
    ${ref("raw_onestock_orders")}  o,UNNEST(line_item_groups) lig
    LEFT JOIN item_id_skus_names i USING (item_id)
    LEFT JOIN layer2_products_pricing_amount pa ON pa.item_sku = i.item_sku AND pa.order_id = o.id
    LEFT JOIN products_taxes_amount pt ON pt.item_sku = i.item_sku AND pt.order_id = o.id
    LEFT JOIN ${ref("raw_magento_orders")} mo ON o.id = mo.increment_id
  WHERE
    delivery.type = 'ckclong_ckclong'
    AND
    mo.status = 'noshow_engraving'
    AND
    lig.state IN ('issue','removed')
  GROUP BY
    order_id,
    date,
    datetime,
    store_id,
    delivery_type,
    item_sku,
    order_last_update_datetime,
    state,
    refusal_reason

  UNION ALL

  SELECT
    o.id AS order_id,
    DATE(DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris')) AS date,
    DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris') AS datetime,
    'ckc_express' AS delivery_type,
    CASE WHEN ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) < 0 THEN ROUND(SUM(adjusted_product_price),2) ELSE ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) END AS ht_revenues,
    ROUND(SUM(adjusted_product_price),2) AS ttc_revenues,
    i.item_sku,   
    DATETIME(TIMESTAMP_SECONDS(o.last_update),'Europe/Paris') AS order_last_update_datetime,
    lig.state,
    mo.status AS refusal_reason,
    delivery.destination.endpoint_id AS store_id,
    CAST(NULL AS STRING) AS return_store_name
  FROM
    ${ref("raw_onestock_orders")}  o,UNNEST(line_item_groups) lig
    LEFT JOIN item_id_skus_names i USING (item_id)
    LEFT JOIN layer2_products_pricing_amount pa ON pa.item_sku = i.item_sku AND pa.order_id = o.id
    LEFT JOIN products_taxes_amount pt ON pt.item_sku = i.item_sku AND pt.order_id = o.id
    LEFT JOIN ${ref("raw_magento_orders")} mo ON o.id = mo.increment_id
  WHERE
    delivery.type = 'ckcexpress_ckcexpress'
    AND
    mo.status = 'noshow_engraving'
    AND
    lig.state IN ('issue','removed')
  GROUP BY
    order_id,
    date,
    datetime,
    store_id,
    delivery_type,
    item_sku,
    order_last_update_datetime,
    state,
    refusal_reason
),

preanalysed_returns_product_scope AS (
  SELECT
    o.id AS order_id,
    DATE(DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris')) AS date,
    DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris') AS datetime,
    'ckc_long' AS delivery_type,
    CASE WHEN ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) < 0 THEN ROUND(SUM(adjusted_product_price),2) ELSE ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) END AS ht_revenues,
    ROUND(SUM(adjusted_product_price),2) AS ttc_revenues,
    i.item_sku,
    DATETIME(TIMESTAMP_SECONDS(o.last_update),'Europe/Paris') AS order_last_update_datetime,
    lig.state,
    CAST(NULL AS STRING) AS refusal_reason,
    delivery.destination.endpoint_id AS store_id,
    rs.store_name AS return_store_name
  FROM
    ${ref("raw_onestock_orders")}  o,UNNEST(line_item_groups) lig
    LEFT JOIN item_id_skus_names i USING (item_id)
    LEFT JOIN ${ref("raw_onestock_history")} oh ON oh.object_id = lig.order_item_id
    LEFT JOIN stores rs ON rs.store_id = JSON_EXTRACT_SCALAR(oh.params,'$.endpoint_id')
    LEFT JOIN layer2_products_pricing_amount pa ON pa.item_sku = i.item_sku AND pa.order_id = o.id
    LEFT JOIN products_taxes_amount pt ON pt.item_sku = i.item_sku AND pt.order_id = o.id
  WHERE
    delivery.type = 'ckclong_ckclong'
    AND
    lig.state = 'returned'
    AND
    JSON_EXTRACT_SCALAR(oh.params,'$.to_state') = 'returned'    
  GROUP BY
    order_id,
    date,
    datetime,
    store_id,
    return_store_name,
    delivery_type,
    item_sku,
    order_last_update_datetime,
    state,
    refusal_reason

  UNION ALL

  SELECT
    o.id AS order_id,
    DATE(DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris')) AS date,
    DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris') AS datetime,
    'ckc_express' AS delivery_type,
    CASE WHEN ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) < 0 THEN ROUND(SUM(adjusted_product_price),2) ELSE ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) END AS ht_revenues,
    ROUND(SUM(adjusted_product_price),2) AS ttc_revenues,
    i.item_sku,   
    DATETIME(TIMESTAMP_SECONDS(o.last_update),'Europe/Paris') AS order_last_update_datetime,
    lig.state,
    CAST(NULL AS STRING) AS refusal_reason,
    delivery.destination.endpoint_id AS store_id,
    rs.store_name AS return_store_name
  FROM
    ${ref("raw_onestock_orders")}  o,UNNEST(line_item_groups) lig
    LEFT JOIN item_id_skus_names i USING (item_id)
    LEFT JOIN ${ref("raw_onestock_history")} oh ON oh.object_id = lig.order_item_id
    LEFT JOIN stores rs ON rs.store_id = JSON_EXTRACT_SCALAR(oh.params,'$.endpoint_id')
    LEFT JOIN layer2_products_pricing_amount pa ON pa.item_sku = i.item_sku AND pa.order_id = o.id
    LEFT JOIN products_taxes_amount pt ON pt.item_sku = i.item_sku AND pt.order_id = o.id
  WHERE
    delivery.type = 'ckcexpress_ckcexpress'
    AND
    lig.state = 'returned'
    AND
    JSON_EXTRACT_SCALAR(oh.params,'$.to_state') = 'returned'     
  GROUP BY
    order_id,
    date,
    datetime,
    store_id,
    return_store_name,
    delivery_type,
    item_sku,
    order_last_update_datetime,
    state,
    refusal_reason
),

preanalysed_returns_return_date_product_scope AS (
  SELECT
    o.id AS order_id,
    DATE(DATETIME(oh.created_at,'Europe/Paris')) AS date,
    DATETIME(oh.created_at,'Europe/Paris') AS datetime,
    'ckc_long' AS delivery_type,
    CASE WHEN ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) < 0 THEN ROUND(SUM(adjusted_product_price),2) ELSE ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) END AS ht_revenues,
    ROUND(SUM(adjusted_product_price),2) AS ttc_revenues,
    i.item_sku,
    DATETIME(TIMESTAMP_SECONDS(o.last_update),'Europe/Paris') AS order_last_update_datetime,
    lig.state,
    CAST(NULL AS STRING) AS refusal_reason,
    delivery.destination.endpoint_id AS store_id,
    rs.store_name AS return_store_name
  FROM
    ${ref("raw_onestock_orders")}  o,UNNEST(line_item_groups) lig
    LEFT JOIN item_id_skus_names i USING (item_id)
    LEFT JOIN ${ref("raw_onestock_history")} oh ON oh.object_id = lig.order_item_id
    LEFT JOIN stores rs ON rs.store_id = JSON_EXTRACT_SCALAR(oh.params,'$.endpoint_id')
    LEFT JOIN layer2_products_pricing_amount pa ON pa.item_sku = i.item_sku AND pa.order_id = o.id
    LEFT JOIN products_taxes_amount pt ON pt.item_sku = i.item_sku AND pt.order_id = o.id
  WHERE
    delivery.type = 'ckclong_ckclong'
    AND
    lig.state = 'returned'
    AND
    JSON_EXTRACT_SCALAR(oh.params,'$.to_state') = 'returned'    
  GROUP BY
    order_id,
    date,
    datetime,
    store_id,
    return_store_name,
    delivery_type,
    item_sku,
    order_last_update_datetime,
    state,
    refusal_reason

  UNION ALL

  SELECT
    o.id AS order_id,
    DATE(DATETIME(oh.created_at,'Europe/Paris')) AS date,
    DATETIME(oh.created_at,'Europe/Paris') AS datetime,
    'ckc_express' AS delivery_type,
    CASE WHEN ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) < 0 THEN ROUND(SUM(adjusted_product_price),2) ELSE ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) END AS ht_revenues,
    ROUND(SUM(adjusted_product_price),2) AS ttc_revenues,
    i.item_sku,   
    DATETIME(TIMESTAMP_SECONDS(o.last_update),'Europe/Paris') AS order_last_update_datetime,
    lig.state,
    CAST(NULL AS STRING) AS refusal_reason,
    delivery.destination.endpoint_id AS store_id,
    rs.store_name AS return_store_name
  FROM
    ${ref("raw_onestock_orders")}  o,UNNEST(line_item_groups) lig
    LEFT JOIN item_id_skus_names i USING (item_id)
    LEFT JOIN ${ref("raw_onestock_history")} oh ON oh.object_id = lig.order_item_id
    LEFT JOIN stores rs ON rs.store_id = JSON_EXTRACT_SCALAR(oh.params,'$.endpoint_id')
    LEFT JOIN layer2_products_pricing_amount pa ON pa.item_sku = i.item_sku AND pa.order_id = o.id
    LEFT JOIN products_taxes_amount pt ON pt.item_sku = i.item_sku AND pt.order_id = o.id
  WHERE
    delivery.type = 'ckcexpress_ckcexpress'
    AND
    lig.state = 'returned'
    AND
    JSON_EXTRACT_SCALAR(oh.params,'$.to_state') = 'returned'     
  GROUP BY
    order_id,
    date,
    datetime,
    store_id,
    return_store_name,
    delivery_type,
    item_sku,
    order_last_update_datetime,
    state,
    refusal_reason
),

final_layer AS (
  SELECT
    *,
    'order' AS scope
  FROM 
    preanalysed_refusals_order_scope
    LEFT JOIN stores USING (store_id)

  UNION ALL

  SELECT
    *,
    'product' AS scope
  FROM
    preanalysed_refusals_product_scope
    LEFT JOIN stores USING (store_id)

  UNION ALL

  SELECT
    *,
    'no_show' AS scope
  FROM
    preanalysed_no_shows_product_scope
    LEFT JOIN stores USING (store_id)

  UNION ALL

  SELECT
    *,
    'return' AS scope
  FROM
    preanalysed_returns_product_scope
    LEFT JOIN stores s USING (store_id)    

  UNION ALL

  SELECT
    *,
    'return_date' AS scope
  FROM
    preanalysed_returns_return_date_product_scope
    LEFT JOIN stores s USING (store_id)    
)

SELECT
  *
FROM
  final_layer

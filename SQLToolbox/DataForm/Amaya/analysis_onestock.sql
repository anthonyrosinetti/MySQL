
-- Config block
config {
  type: "table",
  bigquery: {
    partitionBy: "order_creation_date"
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

ckc_long_items_parcels_scope AS (
  SELECT DISTINCT
    o.id AS order_id,
    MAX(CASE WHEN lig.state = 'claimed_warehouse' THEN True ELSE False END) AS claimed_warehouse,
    MAX(CASE WHEN p.state = 'dispatched' THEN True ELSE False END) AS dispatched
  FROM
    ${ref("raw_onestock_orders")} o,UNNEST(line_item_groups) lig,UNNEST(parcels) p
  WHERE
    o.delivery.type = 'ckclong_ckclong'
  GROUP BY
    order_id
),

home_delivery_scope AS (
  SELECT DISTINCT
    o.id AS order_id,
    mo.status AS state,
    MAX(p.delivery.origin.endpoint_id) AS store_id,
  FROM
    ${ref("raw_onestock_orders")} o,UNNEST(parcels) p
    LEFT JOIN
      ${ref("raw_magento_orders")} mo
    ON
      o.id = mo.increment_id
  WHERE
      o.delivery.type NOT IN ('ckcexpress_ckcexpress','ckclong_ckclong')
      AND
      mo.status IN ('preparation_in_progress','ready_to_send','complete')
      AND
      mo.status NOT IN ('pending_payment','canceled','test','collab')
  GROUP BY
    order_id,
    state      
),

stores_distinct_open_days AS (
  SELECT DISTINCT
    id AS store_id,
    week_day AS open_day
  FROM
    ${ref("onestock_endpoints_history")}
),

next_open_days_mapping AS (
  SELECT
    store_id,
    open_day,
    COALESCE(
        LEAD(open_day) OVER (PARTITION BY store_id ORDER BY open_day ASC),
        FIRST_VALUE(open_day) OVER (PARTITION BY store_id ORDER BY open_day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    ) AS next_open_day
  FROM
    stores_distinct_open_days
),

opening_hours AS (
  SELECT DISTINCT
    id AS store_id,
    week_day,
    EXTRACT(HOUR FROM DATETIME(TIMESTAMP_SECONDS(week_day_opening))) AS start_hour,
    EXTRACT(MINUTE FROM DATETIME(TIMESTAMP_SECONDS(week_day_opening))) AS start_minute,
    EXTRACT(HOUR FROM DATETIME(TIMESTAMP_SECONDS(week_day_closing))) AS end_hour,
    EXTRACT(MINUTE FROM DATETIME(TIMESTAMP_SECONDS(week_day_closing))) AS end_minute,
    COALESCE(
        LEAD(EXTRACT(HOUR FROM DATETIME(TIMESTAMP_SECONDS(week_day_opening)))) OVER (PARTITION BY id,_ingestion_timestamp ORDER BY EXTRACT(DAY FROM DATETIME(TIMESTAMP_SECONDS(week_day_opening))) ASC),
        FIRST_VALUE(EXTRACT(HOUR FROM DATETIME (TIMESTAMP_SECONDS(week_day_opening)))) OVER (PARTITION BY id,_ingestion_timestamp ORDER BY EXTRACT(DAY FROM DATETIME(TIMESTAMP_SECONDS(week_day_opening))) ASC) 
    ) AS start_hour_next_day,
    COALESCE(
        LEAD(EXTRACT(MINUTE FROM DATETIME(TIMESTAMP_SECONDS(week_day_opening)))) OVER (PARTITION BY id,_ingestion_timestamp ORDER BY EXTRACT(DAY FROM DATETIME(TIMESTAMP_SECONDS(week_day_opening))) ASC),
        FIRST_VALUE(EXTRACT(MINUTE FROM DATETIME(TIMESTAMP_SECONDS(week_day_opening)))) OVER (PARTITION BY id,_ingestion_timestamp ORDER BY EXTRACT(DAY FROM DATETIME(TIMESTAMP_SECONDS(week_day_opening))) ASC)
    ) AS start_minute_next_day,
    next_open_day,
    MOD(
      next_open_day - EXTRACT(DAY FROM DATETIME(TIMESTAMP_SECONDS(week_day_opening))) + 7
    ,7) AS days_before_next_open_day,
    _ingestion_timestamp
  FROM
    ${ref("onestock_endpoints_history")} h
  LEFT JOIN
    next_open_days_mapping dm
  ON
    dm.store_id = h.id
    AND
    dm.open_day = h.week_day    
),

layer2_opening_hours  AS (
  SELECT
    *,
   -- computing the time during stores off hours to deduce from bagging and collect times
    DATETIME_DIFF(DATETIME(2000,1,2,start_hour_next_day,start_minute_next_day,0),DATETIME(2000,1,1,end_hour,end_minute,0),SECOND) + (days_before_next_open_day-1)*24*3600 AS overnight_time_before_next_open_day
  FROM
    opening_hours
),

orders_off_day AS (
  SELECT DISTINCT
    o.id AS order_id,
    DATETIME(TIMESTAMP_SECONDS(o.date),'Europe/Paris') AS order_creation_date,
    o.delivery.destination.endpoint_id AS store_id,
   -- computing the day of week when the computation of bagging and collect times should be started
    CASE
      WHEN MOD(EXTRACT(DAYOFWEEK FROM DATETIME(TIMESTAMP_SECONDS(o.date),'Europe/Paris'))+5,7)+1 != oh.week_day THEN MIN(oh.week_day) OVER (PARTITION BY oh.store_id,oh._ingestion_timestamp)
      ELSE MOD(EXTRACT(DAYOFWEEK FROM DATETIME(TIMESTAMP_SECONDS(o.date),'Europe/Paris'))+5,7)+1
    END AS true_day_of_week,
   -- computing whether or not the order has been made on an ON or an OFF day
    CASE
      WHEN MOD(EXTRACT(DAYOFWEEK FROM DATETIME(TIMESTAMP_SECONDS(o.date),'Europe/Paris'))+5,7)+1 != oh.week_day THEN True
      ELSE False
    END AS off_day,         
    DATETIME(TIMESTAMP_SECONDS(o.date),'Europe/Paris') AS order_date,
    EXTRACT(YEAR FROM DATETIME(TIMESTAMP_SECONDS(o.date),'Europe/Paris')) AS order_year,
    EXTRACT(MONTH FROM DATETIME(TIMESTAMP_SECONDS(o.date),'Europe/Paris')) AS order_month,
    EXTRACT(DAY FROM DATETIME(TIMESTAMP_SECONDS(o.date),'Europe/Paris')) AS order_day,
    EXTRACT(HOUR FROM DATETIME(TIMESTAMP_SECONDS(o.date),'Europe/Paris')) AS order_hour,
    EXTRACT(MINUTE FROM DATETIME(TIMESTAMP_SECONDS(o.date),'Europe/Paris')) AS order_minute
  FROM
    ${ref("raw_onestock_orders")} o
    LEFT JOIN layer2_opening_hours oh
    ON
      oh.store_id = o.delivery.destination.endpoint_id
      AND
      oh.week_day = MOD(EXTRACT(DAYOFWEEK FROM DATETIME(TIMESTAMP_SECONDS(o.date),'Europe/Paris'))+5,7)+1
      AND
      DATETIME(oh._ingestion_timestamp) <= DATETIME(TIMESTAMP_SECONDS(o.date),'Europe/Paris')
  WHERE
    delivery.type IN ('ckcexpress_ckcexpress')
),

ckc_express_bagging_times AS (
-- computing orders true processing datetime which consists of next open day starting hour if the order has been made when the store is closed, and of the order date itself otherwise
  SELECT DISTINCT
    o.order_id,
    o.store_id,
    CASE
      WHEN (
        off_day
        OR
        (order_hour < LAST_VALUE(oh.start_hour) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC))
          OR 
        (order_hour = LAST_VALUE(oh.start_hour) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC) AND order_minute < LAST_VALUE(oh.start_minute) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC))
          OR 
        (order_hour > LAST_VALUE(oh.end_hour) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC))
          OR 
        (order_hour = LAST_VALUE(oh.end_hour) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC) AND order_minute > LAST_VALUE(oh.end_minute) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC))                    
      ) THEN False
      WHEN (
        (order_hour > LAST_VALUE(oh.start_hour) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC))
          OR 
        (order_hour = LAST_VALUE(oh.start_hour) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC) AND order_minute > LAST_VALUE(oh.start_minute) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC))
      )
      AND
      (
        (order_hour < LAST_VALUE(oh.end_hour) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC))
          OR 
        (order_hour = LAST_VALUE(oh.end_hour) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC) AND order_minute < LAST_VALUE(oh.end_minute) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC))
      ) THEN True
    ELSE NULL END AS during_opening_hours,-- computing whether or not the order has been made during stores opening hours or not
    oh.week_day,
    o.true_day_of_week,
    CASE
      WHEN off_day THEN DATETIME(
        EXTRACT(YEAR FROM DATE_ADD(order_date,INTERVAL
        MOD(
          true_day_of_week - (MOD(EXTRACT(DAYOFWEEK FROM order_date)+5,7)+1)+7,7
          )
        DAY)),
        EXTRACT(MONTH FROM DATE_ADD(order_date,INTERVAL
        MOD(
          true_day_of_week - (MOD(EXTRACT(DAYOFWEEK FROM order_date)+5,7)+1)+7,7
        )
        DAY)),
        EXTRACT(DAY FROM DATE_ADD(order_date,INTERVAL
        MOD(
          true_day_of_week - (MOD(EXTRACT(DAYOFWEEK FROM order_date)+5,7)+1)+7,7
        )
        DAY)),
        LAST_VALUE(oh.start_hour) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC),
        LAST_VALUE(oh.start_minute) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC),
        0)
      WHEN (
        (order_hour < LAST_VALUE(oh.start_hour) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC))
          OR 
        (order_hour = LAST_VALUE(oh.start_hour) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC) AND order_minute < LAST_VALUE(oh.start_minute) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC))
      ) THEN DATETIME(order_year,order_month,order_day,LAST_VALUE(oh.start_hour) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC),LAST_VALUE(oh.start_minute) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC),0)
      WHEN (
        (order_hour > LAST_VALUE(oh.start_hour) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC))
          OR 
        (order_hour = LAST_VALUE(oh.start_hour) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC) AND order_minute > LAST_VALUE(oh.start_minute) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC))
      )
      AND
      (
        (order_hour < LAST_VALUE(oh.end_hour) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC))
          OR 
        (order_hour = LAST_VALUE(oh.end_hour) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC) AND order_minute < LAST_VALUE(oh.end_minute) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC))
      ) THEN order_date 
      WHEN (
        (order_hour > LAST_VALUE(oh.end_hour) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC))
          OR 
        (order_hour = LAST_VALUE(oh.end_hour) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC) AND order_minute > LAST_VALUE(oh.end_minute) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC))
      ) THEN DATETIME_ADD(DATETIME(order_year,order_month,order_day,LAST_VALUE(oh.start_hour_next_day) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC),LAST_VALUE(oh.start_minute_next_day) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC),0),INTERVAL LAST_VALUE(oh.days_before_next_open_day) OVER (PARTITION BY oh.store_id,oh.week_day ORDER BY _ingestion_timestamp ASC) DAY)
    ELSE NULL END AS order_true_processing_start_datetime,-- computing the true processing datetime to be considered when computing bagging times, according to whether the order has been made during opening hours or not
    CAST(NULL AS DATETIME) AS to_collectable_datetime
  FROM
    orders_off_day o
    LEFT JOIN layer2_opening_hours oh
      ON
        oh.store_id = o.store_id
        AND
        oh.week_day = o.true_day_of_week
        AND
        DATETIME(oh._ingestion_timestamp) <= order_date
  
  UNION ALL

-- unioning collectable times for all orders
  SELECT DISTINCT
    order_id,
    store_id,
    CAST(NULL AS BOOLEAN) AS during_open_hours,
    CAST(NULL AS INT64) AS week_day,
    CAST(NULL AS INT64) AS true_day_of_week,
    CAST(NULL AS DATETIME) AS order_true_processing_start_datetime,
    DATETIME(created_at,'Europe/Paris') AS to_collectable_datetime
  FROM
    ${ref("raw_onestock_history")}
    LEFT JOIN orders_off_day o USING (order_id)
  WHERE
    action = 'change_state'
    AND
    object_type = 'order'
    AND
    JSON_EXTRACT_SCALAR(params,'$.to_state') = 'collectable'
),

-- computing times between true processing datetimes and collectable datetimes as unadjusted_bagging_time, without considering overnight OFF hours at that stage
layer2_ckc_express_bagging_times AS (
  SELECT
  order_id,
  MAX(store_id) AS store_id,
  MAX(true_day_of_week) AS true_day_of_week,
  MAX(CASE
      WHEN during_opening_hours OR during_opening_hours IS NULL THEN DATETIME(created_at,'Europe/Paris')
      ELSE order_true_processing_start_datetime
    END) AS order_true_processing_start_datetime,
  DATETIME_DIFF(MIN(to_collectable_datetime),MAX(CASE
      WHEN during_opening_hours OR during_opening_hours IS NULL THEN DATETIME(created_at,'Europe/Paris')
      ELSE order_true_processing_start_datetime
    END),SECOND) AS unadjusted_bagging_time,
-- computing the number of days between processing and collectable datetimes to be able to assess cumulated overnight hours to substract in next CTE
  DATE_DIFF(MIN(to_collectable_datetime),MAX(CASE
      WHEN during_opening_hours OR during_opening_hours IS NULL THEN DATETIME(created_at,'Europe/Paris')
      ELSE order_true_processing_start_datetime
    END),DAY) AS days_difference
FROM
  ckc_express_bagging_times cb
  LEFT JOIN ${ref("raw_onestock_history")} h USING (order_id)
WHERE
  action = 'change_state'
  AND
  object_type = 'order'
  AND
  JSON_EXTRACT_SCALAR(params,'$.to_state') = 'processing'
GROUP BY
  order_id
),

-- substracting cumulated OFF hours over days to the unadjusted bagging times
layer3_ckc_express_bagging_times AS (
  SELECT
    order_id,
    cb.store_id,  
    MAX(unadjusted_bagging_time) - SUM(CASE WHEN days_difference > 0 THEN overnight_time_before_next_open_day ELSE 0 END) AS bagging_time,
  FROM
    layer2_ckc_express_bagging_times cb
    LEFT JOIN layer2_opening_hours oh
      ON
        oh.store_id = cb.store_id
        AND
        oh.week_day >= cb.true_day_of_week
        AND
        oh.week_day <= MOD(true_day_of_week + days_difference - 1,7)+1
        AND
        DATE(oh._ingestion_timestamp) <= order_true_processing_start_datetime
  GROUP BY
    order_id,
    store_id,
    week_day,
    _ingestion_timestamp        
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY store_id,order_id,week_day ORDER BY DATETIME(oh._ingestion_timestamp) DESC) = 1         
),

orders_timings_scope AS (
-- computing the preparation datetimes for reception time computation for CKC long
  SELECT DISTINCT
    order_id,
    CAST(NULL AS DATETIME) AS created_datetime,
    CAST(NULL AS DATETIME) AS to_processing_datetime,
    DATE_SUB(DATETIME(EXTRACT(YEAR FROM DATETIME(created_at,'Europe/Paris')),EXTRACT(MONTH FROM DATETIME(created_at,'Europe/Paris')),EXTRACT(DAY FROM DATETIME(created_at,'Europe/Paris')),16,0,0),
INTERVAL
    MOD(
      MOD(EXTRACT(DAYOFWEEK FROM DATETIME(created_at,'Europe/Paris'))+5,7)+1 - COALESCE(MAX(CASE WHEN sw.week_day <= MOD(EXTRACT(DAYOFWEEK FROM DATETIME(created_at,'Europe/Paris'))+5,7)+1 THEN sw.week_day ELSE NULL END) OVER (PARTITION BY o.id),MAX(sw.week_day) OVER (PARTITION BY o.id)) + 7
    ,7)
                DAY) AS to_preparation_datetime,
    CAST(NULL AS DATETIME) AS to_collectable_datetime,
    CAST(NULL AS DATETIME) AS to_fulfilled_datetime,
    'to_preparation' AS step
  FROM
    ${ref("raw_onestock_history")} h
    LEFT JOIN ${ref("raw_onestock_orders")} o ON o.id = h.order_id
    LEFT JOIN ${ref("stores_shipment_weekdays")} sw
      ON
        sw.id = o.delivery.destination.endpoint_id
        AND
        ((DATE(created_at,'Europe/Paris') >= sw.valid_start_date) OR (DATE(created_at,'Europe/Paris') BETWEEN sw.valid_start_date AND sw.valid_end_date))      
  WHERE
    action = 'change_state'
    AND
    object_type = 'order'
    AND
    JSON_EXTRACT_SCALAR(params,'$.to_state') = 'collectable'

  UNION ALL

-- computing the collectable datetimes for all orders
  SELECT DISTINCT
    order_id,
    CAST(NULL AS DATETIME) AS created_datetime,
    CAST(NULL AS DATETIME) AS to_processing_datetime,
    CAST(NULL AS DATETIME) AS to_preparation_datetime,
    DATETIME(created_at,'Europe/Paris') AS to_collectable_datetime,
    CAST(NULL AS DATETIME) AS to_fulfilled_datetime,
    'to_collectable' AS step
  FROM
    ${ref("raw_onestock_history")}
  WHERE
    action = 'change_state'
    AND
    object_type = 'order'
    AND
    JSON_EXTRACT_SCALAR(params,'$.to_state') = 'collectable'

  UNION ALL

-- computing the fulfilled datetimes for all orders
  SELECT DISTINCT
    order_id,
    CAST(NULL AS DATETIME) AS created_datetime,
    CAST(NULL AS DATETIME) AS to_processing_datetime,
    CAST(NULL AS DATETIME) AS to_preparation_datetime,
    CAST(NULL AS DATETIME) AS to_collectable_datetime,
    DATETIME(created_at,'Europe/Paris')  AS to_fulfilled_datetime,
    'to_fulfilled' AS step
  FROM
    ${ref("raw_onestock_history")}
  WHERE
    action = 'change_state'
    AND
    object_type = 'order'
    AND
    JSON_EXTRACT_SCALAR(params,'$.to_state') = 'fulfilled'

  UNION ALL

-- computing the complete datetimes for computing the home deliveries whole preparation times
  SELECT DISTINCT
    increment_id AS order_id,
    DATETIME_ADD(DATETIME(o.created_at,'Europe/Paris'),INTERVAL 1 HOUR) AS created_datetime,
    CAST(NULL AS DATETIME) AS to_processing_datetime,
    CAST(NULL AS DATETIME) AS to_preparation_datetime,
    CAST(NULL AS DATETIME) AS to_collectable_datetime,
    DATETIME(sh.created_at,'Europe/Paris') AS to_fulfilled_datetime,
    'complete' AS step
  FROM ${ref("raw_magento_orders")} o,UNNEST(status_histories) sh
  WHERE
    sh.status = 'complete'
    AND
    EXTRACT(YEAR FROM TIMESTAMP_TRUNC(o.created_at,YEAR)) >= 2025
),

collect_times_scope AS (
  SELECT DISTINCT
    order_id,
    delivery.destination.endpoint_id AS store_id,
    MOD(EXTRACT(DAYOFWEEK FROM DATETIME(TIMESTAMP_SECONDS(o.date),'Europe/Paris'))+5,7)+1 AS true_day_of_week,
    DATETIME(created_at,'Europe/Paris') AS to_collectable_datetime,
    CAST(NULL AS DATETIME) AS to_fulfilled_datetime
  FROM
    ${ref("raw_onestock_history")} h
    LEFT JOIN ${ref("raw_onestock_orders")} o ON o.id = h.order_id
  WHERE
    action = 'change_state'
    AND
    object_type = 'order'
    AND
    JSON_EXTRACT_SCALAR(params,'$.to_state') = 'collectable'

  UNION ALL

  SELECT DISTINCT
    order_id,
    delivery.destination.endpoint_id AS store_id,
    MOD(EXTRACT(DAYOFWEEK FROM DATETIME(TIMESTAMP_SECONDS(o.date),'Europe/Paris'))+5,7)+1 AS true_day_of_week,
    CAST(NULL AS DATETIME) AS to_collectable_datetime,
    DATETIME(created_at,'Europe/Paris')  AS to_fulfilled_datetime
  FROM
    ${ref("raw_onestock_history")} h
    LEFT JOIN ${ref("raw_onestock_orders")} o ON o.id = h.order_id
  WHERE
    action = 'change_state'
    AND
    object_type = 'order'
    AND
    JSON_EXTRACT_SCALAR(params,'$.to_state') = 'fulfilled'
),

layer2_collect_times_scope AS (
  SELECT
  order_id,
  MAX(store_id) AS store_id,
  MAX(true_day_of_week) AS true_day_of_week,
  MAX(to_collectable_datetime) AS to_collectable_datetime,
  DATETIME_DIFF(MIN(to_fulfilled_datetime),MAX(to_collectable_datetime),SECOND) AS unadjusted_collect_time,
  DATE_DIFF(MIN(to_fulfilled_datetime),MAX(to_collectable_datetime),DAY) AS days_difference
FROM
  collect_times_scope cb
GROUP BY
  order_id
),

layer3_collect_times_scope AS (
  SELECT
    order_id,
    cb.store_id,
    MAX(unadjusted_collect_time) - SUM(CASE WHEN days_difference > 0 THEN overnight_time_before_next_open_day ELSE 0 END) AS collect_time
  FROM
    layer2_collect_times_scope cb
    LEFT JOIN layer2_opening_hours oh
      ON
        oh.store_id = cb.store_id
        AND
        oh.week_day >= cb.true_day_of_week
        AND
        oh.week_day <= MOD(true_day_of_week + days_difference - 1,7)+1        
        AND
        DATETIME(oh._ingestion_timestamp) <= to_collectable_datetime
  GROUP BY
    order_id,
    store_id,
    week_day,
    _ingestion_timestamp    
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY store_id,order_id,week_day ORDER BY DATETIME(oh._ingestion_timestamp) DESC) = 1     
),

orders_timings AS (
  SELECT DISTINCT
    order_id,
    CASE WHEN MAX(l3.bagging_time) < 0 THEN 0 ELSE MAX(l3.bagging_time) END AS bagging_time,
    DATETIME_DIFF(MIN(to_collectable_datetime),MAX(to_preparation_datetime), SECOND) AS reception_time,
    CASE WHEN MAX(l3_bis.collect_time) < 0 THEN 0 ELSE MAX(l3_bis.collect_time) END AS collect_time,
    DATETIME_DIFF(MIN(to_fulfilled_datetime),MAX(created_datetime), SECOND) AS creation_to_fulfilled_time
  FROM
    orders_timings_scope
    LEFT JOIN layer3_ckc_express_bagging_times l3 USING (order_id)
    LEFT JOIN layer3_collect_times_scope l3_bis USING (order_id)
  GROUP BY
    order_id
),

orders_taxes_amount AS (
  SELECT DISTINCT
    o.id AS order_id,
    SUM(t.amount) AS order_taxes
  FROM
    ${ref("raw_onestock_orders")} o,UNNEST(order_items) oi,UNNEST(oi.pricing_details.taxes) t
  GROUP BY
    order_id
),

products_pricing_amount AS (
  SELECT DISTINCT
    JSON_EXTRACT_SCALAR(oi.information,'$.gtin') AS item_sku,
    o.id AS order_id,
    MAX(o.pricing_details.price) OVER (PARTITION BY o.id) AS order_price,
    SUM(oi.pricing_details.price) OVER (PARTITION BY o.id, JSON_EXTRACT_SCALAR(oi.information,'$.gtin')) AS product_price,
    SUM(oi.pricing_details.price) OVER (PARTITION BY o.id) AS total_product_price,
  FROM
    ${ref("raw_onestock_orders")} o,UNNEST(order_items) oi
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
    ${ref("raw_onestock_orders")} o,UNNEST(order_items) oi,UNNEST(oi.pricing_details.taxes) t
  GROUP BY
    item_sku,
    order_id
),

preanalysed_orders_scope AS (
  SELECT
    increment_id AS order_id,
    DATE(DATETIME(created_at,'Europe/Paris')) AS order_creation_date,
    CAST(NULL AS STRING) AS delivery_type,
    ROUND(SUM(base_grand_total)-SUM(base_tax_amount)-SUM(base_tax_refunded),2) AS ht_revenues,
    ROUND(SUM(base_grand_total),2) AS ttc_revenues,
    CAST(NULL AS STRING) AS item_name,
    CAST(NULL AS STRING) AS item_sku,    
    status AS order_state,
    DATETIME(updated_at,'Europe/Paris') AS order_last_update_datetime,
    CAST(NULL AS INT64) AS bagging_time,
    CAST(NULL AS INT64) AS collect_time,
    CAST(NULL AS INT64) AS reception_time,
    CAST(NULL AS INT64) AS creation_to_fulfilled_time,
    'W0001' AS store_id,
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
    order_creation_date,
    store_id,
    delivery_type,
    item_name,
    item_sku,
    order_state,
    order_last_update_datetime,
    bagging_time,
    collect_time,
    reception_time,
    creation_to_fulfilled_time

  UNION ALL

  SELECT
    id AS order_id,
    DATE(DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris')) AS order_creation_date,
    'ckc_long' AS delivery_type,
    CASE WHEN ROUND(SUM(pricing_details.price)-SUM(t.order_taxes),2) < 0 THEN ROUND(SUM(pricing_details.price),2) ELSE ROUND(SUM(pricing_details.price)-SUM(t.order_taxes),2) END AS ht_revenues,
    ROUND(SUM(pricing_details.price),2) AS ttc_revenues,
    CAST(NULL AS STRING) AS item_name,
    CAST(NULL AS STRING) AS item_sku,    
    CASE
      WHEN o.state = 'processing' AND ip.claimed_warehouse THEN 'not_prepared'
      WHEN ip.dispatched THEN 'picked'
      ELSE o.state
    END AS order_state,
    DATETIME(TIMESTAMP_SECONDS(last_update),'Europe/Paris') AS order_last_update_datetime,
    CAST(NULL AS INT64) AS bagging_time,
    ob.collect_time,
    ob.reception_time,
    CAST(NULL AS INT64) AS creation_to_fulfilled_time,
    delivery.destination.endpoint_id AS store_id,
  FROM
    ${ref("raw_onestock_orders")} o
    LEFT JOIN ${ref("raw_magento_orders")} mo ON o.id = mo.increment_id      
    LEFT JOIN orders_taxes_amount t ON t.order_id = o.id
    LEFT JOIN orders_timings ob USING (order_id)
    LEFT JOIN ckc_long_items_parcels_scope ip USING (order_id)
  WHERE
    delivery.type = 'ckclong_ckclong'
    AND
    mo.status NOT IN ('pending_payment','canceled','test','collab')      
  GROUP BY
    order_id,
    order_creation_date,
    store_id,
    delivery_type,
    item_name,
    item_sku,
    order_state,
    order_last_update_datetime,
    bagging_time,
    collect_time,
    reception_time,
    creation_to_fulfilled_time

  UNION ALL

  SELECT
    id AS order_id,
    DATE(DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris')) AS order_creation_date,
    'ckc_express' AS delivery_type,
    CASE WHEN ROUND(SUM(pricing_details.price)-SUM(t.order_taxes),2) < 0 THEN ROUND(SUM(pricing_details.price),2) ELSE ROUND(SUM(pricing_details.price)-SUM(t.order_taxes),2) END AS ht_revenues,
    ROUND(SUM(pricing_details.price),2) AS ttc_revenues,
    CAST(NULL AS STRING) AS item_name,
    CAST(NULL AS STRING) AS item_sku,    
    o.state AS order_state,
    DATETIME(TIMESTAMP_SECONDS(last_update),'Europe/Paris') AS order_last_update_datetime,
    ob.bagging_time,
    ob.collect_time,
    CAST(NULL AS INT64) AS reception_time,
    CAST(NULL AS INT64) AS creation_to_fulfilled_time,
    delivery.destination.endpoint_id AS store_id,
  FROM
    ${ref("raw_onestock_orders")} o
    LEFT JOIN ${ref("raw_magento_orders")} mo ON o.id = mo.increment_id
    LEFT JOIN orders_taxes_amount t ON t.order_id = o.id
    LEFT JOIN orders_timings ob USING (order_id)
  WHERE
    delivery.type = 'ckcexpress_ckcexpress'
    AND
    mo.status NOT IN ('pending_payment','canceled','test','collab')      
  GROUP BY
    order_id,
    order_creation_date,
    store_id,
    delivery_type,
    item_name,
    item_sku,
    order_state,
    order_last_update_datetime,
    bagging_time,
    collect_time,
    reception_time,
    creation_to_fulfilled_time

  UNION ALL

  SELECT
    id AS order_id,
    DATE(DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris')) AS order_creation_date,
    'home_delivery' AS delivery_type,
    CASE WHEN ROUND(SUM(pricing_details.price)-SUM(t.order_taxes),2) < 0 THEN ROUND(SUM(pricing_details.price),2) ELSE ROUND(SUM(pricing_details.price)-SUM(t.order_taxes),2) END AS ht_revenues,
    ROUND(SUM(pricing_details.price),2) AS ttc_revenues,
    CAST(NULL AS STRING) AS item_name,
    CAST(NULL AS STRING) AS item_sku,
    lad.state AS order_state,
    DATETIME(TIMESTAMP_SECONDS(last_update),'Europe/Paris') AS order_last_update_datetime,
    CAST(NULL AS INT64) AS bagging_time,
    CAST(NULL AS INT64) AS collect_time,
    CAST(NULL AS INT64) AS reception_time,
    ob.creation_to_fulfilled_time,
    lad.store_id,
  FROM
    ${ref("raw_onestock_orders")} o
    INNER JOIN home_delivery_scope lad ON lad.order_id = o.id
    LEFT JOIN orders_taxes_amount t ON t.order_id = o.id
    LEFT JOIN orders_timings ob ON ob.order_id = o.id
  GROUP BY
    order_id,
    order_creation_date,
    store_id,
    delivery_type,
    item_name,
    item_sku,
    order_state,
    order_last_update_datetime,
    bagging_time,
    collect_time,
    reception_time,
    creation_to_fulfilled_time
),

preanalysed_products_scope AS (
  SELECT
    o.id AS order_id,
    DATE(DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris')) AS order_creation_date,
    'ckc_long' AS delivery_type,
    CASE WHEN ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) < 0 THEN ROUND(SUM(adjusted_product_price),2) ELSE ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) END AS ht_revenues,
    ROUND(SUM(adjusted_product_price),2) AS ttc_revenues,
    JSON_EXTRACT_SCALAR(oi.information,'$.product_name') AS item_name,
    JSON_EXTRACT_SCALAR(oi.information,'$.gtin') AS item_sku,
    CASE
      WHEN state = 'processing' AND ip.claimed_warehouse THEN 'not_prepared'
      WHEN ip.dispatched THEN 'picked'
      ELSE state
    END AS order_state,
    DATETIME(TIMESTAMP_SECONDS(last_update),'Europe/Paris') AS order_last_update_datetime,
    CAST(NULL AS INT64) AS bagging_time,
    CAST(NULL AS INT64) AS collect_time,
    CAST(NULL AS INT64) AS reception_time,
    CAST(NULL AS INT64) AS creation_to_fulfilled_time,      
    delivery.destination.endpoint_id AS store_id,
  FROM
    ${ref("raw_onestock_orders")} o,UNNEST(order_items) oi
    LEFT JOIN layer2_products_pricing_amount pa ON pa.item_sku = JSON_EXTRACT_SCALAR(oi.information,'$.gtin') AND pa.order_id = o.id
    LEFT JOIN products_taxes_amount pt ON pt.item_sku = JSON_EXTRACT_SCALAR(oi.information,'$.gtin') AND pt.order_id = o.id
    LEFT JOIN ckc_long_items_parcels_scope ip ON ip.order_id = o.id
  WHERE
    delivery.type = 'ckclong_ckclong'
  GROUP BY
    order_id,
    order_creation_date,
    store_id,
    delivery_type,
    item_name,
    item_sku,
    order_state,
    order_last_update_datetime

  UNION ALL

  SELECT
    o.id AS order_id,
    DATE(DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris')) AS order_creation_date,
    'ckc_express' AS delivery_type,
    CASE WHEN ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) < 0 THEN ROUND(SUM(adjusted_product_price),2) ELSE ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) END AS ht_revenues,
    ROUND(SUM(adjusted_product_price),2) AS ttc_revenues,
    JSON_EXTRACT_SCALAR(oi.information,'$.product_name') AS item_name,
    JSON_EXTRACT_SCALAR(oi.information,'$.gtin') AS item_sku,    
    state AS order_state,
    DATETIME(TIMESTAMP_SECONDS(last_update),'Europe/Paris') AS order_last_update_datetime,
    CAST(NULL AS INT64) AS bagging_time,
    CAST(NULL AS INT64) AS collect_time,
    CAST(NULL AS INT64) AS reception_time,
    CAST(NULL AS INT64) AS creation_to_fulfilled_time,      
    delivery.destination.endpoint_id AS store_id,
  FROM
    ${ref("raw_onestock_orders")} o,UNNEST(order_items) oi
    LEFT JOIN layer2_products_pricing_amount pa ON pa.item_sku = JSON_EXTRACT_SCALAR(oi.information,'$.gtin') AND pa.order_id = o.id
    LEFT JOIN products_taxes_amount pt ON pt.item_sku = JSON_EXTRACT_SCALAR(oi.information,'$.gtin') AND pt.order_id = o.id
  WHERE
    delivery.type = 'ckcexpress_ckcexpress'
  GROUP BY
    order_id,
    order_creation_date,
    store_id,
    delivery_type,
    item_name,
    item_sku,
    order_state,
    order_last_update_datetime    

  UNION ALL

  SELECT
    o.id AS order_id,
    DATE(DATETIME(TIMESTAMP_SECONDS(date),'Europe/Paris')) AS order_creation_date,
    'home_delivery' AS delivery_type,
    CASE WHEN ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) < 0 THEN ROUND(SUM(adjusted_product_price),2) ELSE ROUND(SUM(adjusted_product_price)-SUM(pt.product_taxes),2) END AS ht_revenues,
    ROUND(SUM(adjusted_product_price),2) AS ttc_revenues,
    JSON_EXTRACT_SCALAR(oi.information,'$.product_name') AS item_name,
    JSON_EXTRACT_SCALAR(oi.information,'$.gtin') AS item_sku,
    lad.state AS order_state,
    DATETIME(TIMESTAMP_SECONDS(last_update),'Europe/Paris') AS order_last_update_datetime,
    CAST(NULL AS INT64) AS bagging_time,
    CAST(NULL AS INT64) AS collect_time,
    CAST(NULL AS INT64) AS reception_time,
    CAST(NULL AS INT64) AS creation_to_fulfilled_time,      
    lad.store_id,
  FROM
    ${ref("raw_onestock_orders")} o,UNNEST(order_items) oi
    INNER JOIN home_delivery_scope lad ON lad.order_id = o.id
    LEFT JOIN layer2_products_pricing_amount pa ON pa.item_sku = JSON_EXTRACT_SCALAR(oi.information,'$.gtin') AND pa.order_id = o.id
    LEFT JOIN products_taxes_amount pt ON pt.item_sku = JSON_EXTRACT_SCALAR(oi.information,'$.gtin') AND pt.order_id = o.id
  GROUP BY
    order_id,  
    order_creation_date,
    store_id,
    delivery_type,
    item_name,
    item_sku,
    order_state,
    order_last_update_datetime    
),

final_layer AS (
  SELECT
    *,
    'order' AS scope
  FROM 
    preanalysed_orders_scope
    LEFT JOIN stores USING (store_id)

  UNION ALL

  SELECT
    *,
    'product' AS scope
  FROM
    preanalysed_products_scope
    LEFT JOIN stores USING (store_id)
)

SELECT
  *
FROM
  final_layer

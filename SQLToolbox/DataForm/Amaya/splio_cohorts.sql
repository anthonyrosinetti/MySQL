CREATE OR REPLACE TABLE `amasty-data.analysis.splio_cohorts` AS (
  WITH new_customers AS (
    SELECT DISTINCT
      id,
      DATE_TRUNC(DATE(cf33_cdp_first_order_date),YEAR) AS first_purchase_calendar_year,
      DATE_TRUNC(DATE_ADD(DATE(cf33_cdp_first_order_date),INTERVAL 6 MONTH),YEAR) AS first_purchase_fiscal_year,
      DATE_TRUNC(DATE(cf33_cdp_first_order_date),MONTH) AS first_purchase_month
    FROM
      `amasty-data.splio.raw_splio_contacts` 
  ),

  orders_cohorted AS (
    SELECT
      DATE_DIFF(DATE_TRUNC(DATE_ADD(DATE(created_at),INTERVAL 6 MONTH),YEAR),first_purchase_fiscal_year,YEAR)+1 AS order_fiscal_year_rank,
      DATE_TRUNC(DATE_ADD(DATE(created_at),INTERVAL 6 MONTH),YEAR) AS order_fiscal_year,
      DATE_DIFF(DATE_TRUNC(DATE(created_at),YEAR),first_purchase_calendar_year,YEAR)+1 AS order_calendar_year_rank,
      DATE_TRUNC(DATE(created_at),YEAR) AS order_calendar_year,
      DATE_DIFF(DATE_TRUNC(DATE(created_at),MONTH),first_purchase_month,MONTH)+1 AS order_month_rank,
      DATE_TRUNC(DATE(created_at),MONTH) AS order_month,
      first_purchase_calendar_year,
      first_purchase_fiscal_year,
      first_purchase_month,
      external_id AS order_id,
      contact_id AS customer_id,
      -- IFNULL(total_price,0)+IFNULL(discount_amount,0) AS ca_ttc,
      IFNULL(total_price,0) AS ca_ttc,
      CASE WHEN IFNULL(total_price,0)-IFNULL(tax_amount,0)+IFNULL(discount_amount,0) < 0 THEN 0 ELSE IFNULL(total_price,0)-IFNULL(tax_amount,0)+IFNULL(discount_amount,0) END AS ca_ht,
      'web_w' AS scope
    FROM 
      `amasty-data.splio.raw_splio_orders_with_details` o
      LEFT JOIN new_customers nc ON nc.id = o.contact.id
    WHERE
      -- LOWER(SPLIT(external_id, '-')[OFFSET(0)]) != 'wr'
      status = 'PAID'
      AND
      total_price != 0
      AND
      nc.first_purchase_month >= DATE(2014,1,1)
      AND
      DATE_TRUNC(DATE(created_at),MONTH) >= nc.first_purchase_month      
      AND
      store.name IN ('Boutique - ECommerce','Boutique en ligne')
      
    UNION ALL

    SELECT
      DATE_DIFF(DATE_TRUNC(DATE_ADD(DATE(created_at),INTERVAL 6 MONTH),YEAR),first_purchase_fiscal_year,YEAR)+1 AS order_fiscal_year_rank,
      DATE_TRUNC(DATE_ADD(DATE(created_at),INTERVAL 6 MONTH),YEAR) AS order_fiscal_year,
      DATE_DIFF(DATE_TRUNC(DATE(created_at),YEAR),first_purchase_calendar_year,YEAR)+1 AS order_calendar_year_rank,
      DATE_TRUNC(DATE(created_at),YEAR) AS order_calendar_year,
      DATE_DIFF(DATE_TRUNC(DATE(created_at),MONTH),first_purchase_month,MONTH)+1 AS order_month_rank,
      DATE_TRUNC(DATE(created_at),MONTH) AS order_month,
      first_purchase_calendar_year,
      first_purchase_fiscal_year,
      first_purchase_month,
      external_id AS order_id,
      contact_id AS customer_id,
      IFNULL(total_price,0) AS ca_ttc,
      CASE WHEN IFNULL(total_price,0)-IFNULL(tax_amount,0) < 0 THEN 0 ELSE IFNULL(total_price,0)-IFNULL(tax_amount,0) END AS ca_ht,
      'web_wr' AS scope
    FROM 
      `amasty-data.splio.raw_splio_orders_with_details` o
      LEFT JOIN new_customers nc ON nc.id = o.contact.id
    WHERE
      -- LOWER(SPLIT(external_id, '-')[OFFSET(0)]) = 'wr'
      status IN ('REFUND')
      AND
      total_price != 0
      AND
      nc.first_purchase_month >= DATE(2014,1,1)
      AND
      DATE_TRUNC(DATE(created_at),MONTH) >= nc.first_purchase_month      
      AND
      store.name IN ('Boutique - ECommerce','Boutique en ligne')      

    UNION ALL

    SELECT
      DATE_DIFF(DATE_TRUNC(DATE_ADD(DATE(created_at),INTERVAL 6 MONTH),YEAR),first_purchase_fiscal_year,YEAR)+1 AS order_fiscal_year_rank,
      DATE_TRUNC(DATE_ADD(DATE(created_at),INTERVAL 6 MONTH),YEAR) AS order_fiscal_year,
      DATE_DIFF(DATE_TRUNC(DATE(created_at),YEAR),first_purchase_calendar_year,YEAR)+1 AS order_calendar_year_rank,
      DATE_TRUNC(DATE(created_at),YEAR) AS order_calendar_year,
      DATE_DIFF(DATE_TRUNC(DATE(created_at),MONTH),first_purchase_month,MONTH)+1 AS order_month_rank,
      DATE_TRUNC(DATE(created_at),MONTH) AS order_month,    
      first_purchase_calendar_year,
      first_purchase_fiscal_year,
      first_purchase_month,
      external_id AS order_id,
      contact_id AS customer_id,
      -- IFNULL(total_price,0)-IFNULL(discount_amount,0) AS ca_ttc,
      IFNULL(total_price,0) AS ca_ttc,
      CASE WHEN IFNULL(total_price,0)-IFNULL(tax_amount,0)-IFNULL(discount_amount,0) < 0 THEN 0 ELSE IFNULL(total_price,0)-IFNULL(tax_amount,0)-IFNULL(discount_amount,0) END AS ca_ht,
      'retail' AS scope
    FROM
      `amasty-data.splio.raw_splio_orders_with_details` o
      LEFT JOIN new_customers nc ON nc.id = o.contact.id
    WHERE
      total_price != 0      
      AND
      nc.first_purchase_month >= DATE(2022,7,1)
      AND
      DATE_TRUNC(DATE(created_at),MONTH) >= nc.first_purchase_month      
      AND
      store.name NOT IN ('Boutique - ECommerce','Boutique en ligne')   

    UNION ALL

    SELECT
      DATE_DIFF(DATE_TRUNC(DATE_ADD(DATE(created_at),INTERVAL 6 MONTH),YEAR),first_purchase_fiscal_year,YEAR)+1 AS order_fiscal_year_rank,
      DATE_TRUNC(DATE_ADD(DATE(created_at),INTERVAL 6 MONTH),YEAR) AS order_fiscal_year,
      DATE_DIFF(DATE_TRUNC(DATE(created_at),YEAR),first_purchase_calendar_year,YEAR)+1 AS order_calendar_year_rank,
      DATE_TRUNC(DATE(created_at),YEAR) AS order_calendar_year,
      DATE_DIFF(DATE_TRUNC(DATE(created_at),MONTH),first_purchase_month,MONTH)+1 AS order_month_rank,
      DATE_TRUNC(DATE(created_at),MONTH) AS order_month,    
      first_purchase_calendar_year,
      first_purchase_fiscal_year,
      first_purchase_month,
      external_id AS order_id,
      contact_id AS customer_id, 
      -- IFNULL(total_price,0)-IFNULL(discount_amount,0) AS ca_ttc,
      IFNULL(total_price,0) AS ca_ttc,
      CASE WHEN IFNULL(total_price,0)-IFNULL(tax_amount,0)-IFNULL(discount_amount,0) < 0 THEN 0 ELSE IFNULL(total_price,0)-IFNULL(tax_amount,0)-IFNULL(discount_amount,0) END AS ca_ht,
      'omni_w' AS scope
    FROM
      `amasty-data.splio.raw_splio_orders_with_details` o
      LEFT JOIN new_customers nc ON nc.id = o.contact.id
    WHERE
      LOWER(SPLIT(external_id, '-')[OFFSET(0)]) != 'wr'
      -- status = 'PAID'
      AND
      total_price != 0      
      AND  
      nc.first_purchase_month >= DATE(2022,7,1)
      AND
      DATE_TRUNC(DATE(created_at),MONTH) >= nc.first_purchase_month

    UNION ALL

    SELECT
      DATE_DIFF(DATE_TRUNC(DATE_ADD(DATE(created_at),INTERVAL 6 MONTH),YEAR),first_purchase_fiscal_year,YEAR)+1 AS order_fiscal_year_rank,
      DATE_TRUNC(DATE_ADD(DATE(created_at),INTERVAL 6 MONTH),YEAR) AS order_fiscal_year,
      DATE_DIFF(DATE_TRUNC(DATE(created_at),YEAR),first_purchase_calendar_year,YEAR)+1 AS order_calendar_year_rank,
      DATE_TRUNC(DATE(created_at),YEAR) AS order_calendar_year,
      DATE_DIFF(DATE_TRUNC(DATE(created_at),MONTH),first_purchase_month,MONTH)+1 AS order_month_rank,
      DATE_TRUNC(DATE(created_at),MONTH) AS order_month,    
      first_purchase_calendar_year,
      first_purchase_fiscal_year,
      first_purchase_month,
      external_id AS order_id,
      contact_id AS customer_id, 
      -- IFNULL(total_price,0)-IFNULL(discount_amount,0) AS ca_ttc,
      IFNULL(total_price,0) AS ca_ttc,
      CASE WHEN IFNULL(total_price,0)-IFNULL(tax_amount,0)-IFNULL(discount_amount,0) < 0 THEN 0 ELSE IFNULL(total_price,0)-IFNULL(tax_amount,0)-IFNULL(discount_amount,0) END AS ca_ht,
      'omni_wr' AS scope
    FROM
      `amasty-data.splio.raw_splio_orders_with_details` o
      LEFT JOIN new_customers nc ON nc.id = o.contact.id
    WHERE
      LOWER(SPLIT(external_id, '-')[OFFSET(0)]) = 'wr'
      -- status = 'PAID'
      AND
      total_price != 0      
      AND  
      nc.first_purchase_month >= DATE(2022,7,1)
      AND
      DATE_TRUNC(DATE(created_at),MONTH) >= nc.first_purchase_month        
  ),

  cohorted_metrics AS (
    SELECT
      order_fiscal_year_rank,
      order_fiscal_year,
      order_calendar_year_rank,
      order_calendar_year,
      order_month_rank,
      order_month,
      first_purchase_calendar_year,
      first_purchase_fiscal_year,
      first_purchase_month,
      ROUND(SUM(CASE WHEN scope = 'web_w' THEN ca_ttc ELSE 0 END)-SUM(CASE WHEN scope = 'web_wr' THEN ca_ttc ELSE 0 END),2) AS ca_ttc,
      ROUND(SUM(CASE WHEN scope = 'web_w' THEN ca_ht ELSE 0 END)-SUM(CASE WHEN scope = 'web_wr' THEN ca_ht ELSE 0 END),2) AS ca_ht,
      COUNT(DISTINCT CASE WHEN scope = 'web_w' THEN order_id ELSE NULL END) AS orders_volume,
      COUNT(DISTINCT CASE WHEN scope = 'web_w' THEN customer_id ELSE NULL END) AS unique_clients_volume,
      ROUND(SAFE_DIVIDE(SUM(CASE WHEN scope = 'web_w' THEN ca_ttc ELSE 0 END),COUNT(DISTINCT CASE WHEN scope = 'web_w' THEN order_id ELSE NULL END)),2) AS aov_ttc,
      ROUND(SAFE_DIVIDE(SUM(CASE WHEN scope = 'web_w' THEN ca_ht ELSE 0 END), COUNT(DISTINCT CASE WHEN scope = 'web_w' THEN order_id ELSE NULL END)),2) AS aov_ht,
      'web' AS scope
    FROM
      orders_cohorted
    WHERE
      scope IN ('web_w','web_wr')
    GROUP BY
      order_fiscal_year_rank,
      order_fiscal_year,
      order_calendar_year_rank,
      order_calendar_year,
      order_month_rank,
      order_month,
      first_purchase_calendar_year,
      first_purchase_fiscal_year,
      first_purchase_month,
      scope

    UNION ALL

    SELECT
      order_fiscal_year_rank,
      order_fiscal_year,
      order_calendar_year_rank,
      order_calendar_year,
      order_month_rank,
      order_month,
      first_purchase_calendar_year,
      first_purchase_fiscal_year,
      first_purchase_month,
      ROUND(SUM(ca_ttc),2) AS ca_ttc,
      ROUND(SUM(ca_ht),2) AS ca_ht,
      COUNT(DISTINCT order_id) AS orders_volume,
      COUNT(DISTINCT customer_id) AS unique_clients_volume,
      ROUND(SAFE_DIVIDE(SUM(ca_ttc),COUNT(DISTINCT order_id)),2) AS aov_ttc,
      ROUND(SAFE_DIVIDE(SUM(ca_ht),COUNT(DISTINCT order_id)),2) AS aov_ht,
      scope
    FROM
      orders_cohorted
    WHERE
      scope = 'retail'    
    GROUP BY
      order_fiscal_year_rank,
      order_fiscal_year,
      order_calendar_year_rank,
      order_calendar_year,
      order_month_rank,
      order_month,
      first_purchase_calendar_year,
      first_purchase_fiscal_year,
      first_purchase_month,
      scope

    UNION ALL

    SELECT
      order_fiscal_year_rank,
      order_fiscal_year,
      order_calendar_year_rank,
      order_calendar_year,
      order_month_rank,
      order_month,
      first_purchase_calendar_year,
      first_purchase_fiscal_year,
      first_purchase_month,
      ROUND(SUM(CASE WHEN scope = 'omni_w' THEN ca_ttc ELSE 0 END)-SUM(CASE WHEN scope = 'omni_wr' THEN ca_ttc ELSE 0 END),2) AS ca_ttc,
      ROUND(SUM(CASE WHEN scope = 'omni_w' THEN ca_ht ELSE 0 END)-SUM(CASE WHEN scope = 'omni_wr' THEN ca_ht ELSE 0 END),2) AS ca_ht,
      COUNT(DISTINCT CASE WHEN scope = 'omni_w' THEN order_id ELSE NULL END) AS orders_volume,
      COUNT(DISTINCT CASE WHEN scope = 'omni_w' THEN customer_id ELSE NULL END) AS unique_clients_volume,
      ROUND(SAFE_DIVIDE(SUM(CASE WHEN scope = 'omni_w' THEN ca_ttc ELSE 0 END),COUNT(DISTINCT CASE WHEN scope = 'omni_w' THEN order_id ELSE NULL END)),2) AS aov_ttc,
      ROUND(SAFE_DIVIDE(SUM(CASE WHEN scope = 'omni_w' THEN ca_ht ELSE 0 END), COUNT(DISTINCT CASE WHEN scope = 'omni_w' THEN order_id ELSE NULL END)),2) AS aov_ht,
      'omni' AS scope
    FROM
      orders_cohorted
    WHERE
      scope IN ('omni_w','omni_wr')      
    GROUP BY
      order_fiscal_year_rank,
      order_fiscal_year,
      order_calendar_year_rank,
      order_calendar_year,
      order_month_rank,
      order_month,
      first_purchase_calendar_year,
      first_purchase_fiscal_year,
      first_purchase_month,
      scope
  ),

  cohorted_clients_volume AS (
    SELECT
      *,
      SUM(unique_clients_volume) OVER (PARTITION BY first_purchase_calendar_year,order_calendar_year) AS calendar_year_clients_volume,
      SUM(unique_clients_volume) OVER (PARTITION BY first_purchase_fiscal_year,order_fiscal_year) AS fiscal_year_clients_volume,
      SUM(unique_clients_volume) OVER (PARTITION BY first_purchase_month,order_month) AS month_clients_volume
    FROM
      cohorted_metrics
    WHERE
      scope = 'web'
    
    UNION ALL
    
    SELECT
      *,
      SUM(unique_clients_volume) OVER (PARTITION BY first_purchase_calendar_year,order_calendar_year) AS calendar_year_clients_volume,
      SUM(unique_clients_volume) OVER (PARTITION BY first_purchase_fiscal_year,order_fiscal_year) AS fiscal_year_clients_volume,
      SUM(unique_clients_volume) OVER (PARTITION BY first_purchase_month,order_month) AS month_clients_volume
    FROM
      cohorted_metrics
    WHERE
      scope = 'retail'
    
    UNION ALL

    SELECT
      *,
      SUM(unique_clients_volume) OVER (PARTITION BY first_purchase_calendar_year,order_calendar_year) AS calendar_year_clients_volume,
      SUM(unique_clients_volume) OVER (PARTITION BY first_purchase_fiscal_year,order_fiscal_year) AS fiscal_year_clients_volume,
      SUM(unique_clients_volume) OVER (PARTITION BY first_purchase_month,order_month) AS month_clients_volume   
    FROM
      cohorted_metrics
    WHERE
      scope = 'omni'
  )

  SELECT
    *,
    MAX(calendar_year_clients_volume) OVER (PARTITION BY first_purchase_calendar_year) AS first_purchase_calendar_year_clients_volume,
    MAX(fiscal_year_clients_volume) OVER (PARTITION BY first_purchase_fiscal_year) AS first_purchase_fiscal_year_clients_volume,
    MAX(month_clients_volume) OVER (PARTITION BY first_purchase_month) AS first_purchase_month_clients_volume
  FROM
    cohorted_clients_volume
  WHERE
    scope = 'web'

  UNION ALL

  SELECT
    *,
    MAX(calendar_year_clients_volume) OVER (PARTITION BY first_purchase_calendar_year) AS first_purchase_calendar_year_clients_volume,
    MAX(fiscal_year_clients_volume) OVER (PARTITION BY first_purchase_fiscal_year) AS first_purchase_fiscal_year_clients_volume,
    MAX(month_clients_volume) OVER (PARTITION BY first_purchase_month) AS first_purchase_month_clients_volume
  FROM
    cohorted_clients_volume
  WHERE
    scope = 'retail'

  UNION ALL

  SELECT
    *,
    MAX(calendar_year_clients_volume) OVER (PARTITION BY first_purchase_calendar_year) AS first_purchase_calendar_year_clients_volume,
    MAX(fiscal_year_clients_volume) OVER (PARTITION BY first_purchase_fiscal_year) AS first_purchase_fiscal_year_clients_volume,
    MAX(month_clients_volume) OVER (PARTITION BY first_purchase_month) AS first_purchase_month_clients_volume
  FROM
    cohorted_clients_volume
  WHERE
    scope = 'omni'    
)

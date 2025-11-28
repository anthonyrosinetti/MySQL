CREATE OR REPLACE TABLE `amasty-data.analysis.prios_cohorts` AS (
  WITH new_customers AS (
    SELECT DISTINCT
      code_client,
      DATE_TRUNC(MIN(DATE(date_fin)) OVER w,YEAR) AS first_purchase_calendar_year,
      DATE_TRUNC(MIN(DATE_ADD(DATE(date_fin),INTERVAL 6 MONTH)) OVER w,YEAR) AS first_purchase_fiscal_year,
      DATE_TRUNC(MIN(DATE(date_fin)) OVER w,MONTH) AS first_purchase_month
    FROM
      `amasty-data.prios.raw_prios_tickets` o,UNNEST(details) AS d
    WINDOW w AS (
        PARTITION BY code_client
        ORDER BY DATE(date_fin) ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
  ),

  prios_revenues AS (
    SELECT DISTINCT
      numero_tck_interne,
      IFNULL(SUM(d.quantite*d.pu_net/100000),0) AS ca_ttc,
      IFNULL(SUM(d.quantite*d.puht/100000),0) AS ca_ht
    FROM
      `amasty-data.prios.raw_prios_tickets` o,UNNEST(details) AS d
    WHERE
      o.type_ticket = 'V'
      AND
      d.type_ticket_detail = 'A'
      AND    
      d.pu_net > 0
      AND
      d.quantite > 0
    GROUP BY
      numero_tck_interne
  ),

  orders_cohorted AS (
    SELECT
      DATE_DIFF(DATE_TRUNC(DATE_ADD(DATE(date_fin),INTERVAL 6 MONTH),YEAR),first_purchase_fiscal_year,YEAR)+1 AS order_fiscal_year_rank, 
      DATE_TRUNC(DATE_ADD(DATE(date_fin),INTERVAL 6 MONTH),YEAR) AS order_fiscal_year,
      DATE_DIFF(DATE_TRUNC(DATE(date_fin),YEAR),first_purchase_calendar_year,YEAR)+1 AS order_calendar_year_rank,
      DATE_TRUNC(DATE(date_fin),YEAR) AS order_calendar_year,
      DATE_DIFF(DATE_TRUNC(DATE(date_fin),MONTH),first_purchase_month,MONTH)+1 AS order_month_rank,
      DATE_TRUNC(DATE(date_fin),MONTH) AS order_month,
      first_purchase_calendar_year,
      first_purchase_fiscal_year,
      first_purchase_month,
      o.numero_tck_interne AS order_id,
      o.code_client AS customer_id,
      rev.ca_ttc,
      rev.ca_ht,
      'web' AS scope
    FROM
      `amasty-data.prios.raw_prios_tickets` o
      LEFT JOIN new_customers nc USING (code_client)
      INNER JOIN prios_revenues rev USING (numero_tck_interne)
    WHERE
      nc.first_purchase_month >= DATE(2022,7,1)
      AND
      DATE_TRUNC(DATE(o.date_fin),MONTH) >= nc.first_purchase_month
      AND
      o.code_boutique_externe_vente = 'W0001'

    UNION ALL

    SELECT
      DATE_DIFF(DATE_TRUNC(DATE_ADD(DATE(date_fin),INTERVAL 6 MONTH),YEAR),first_purchase_fiscal_year,YEAR)+1 AS order_fiscal_year_rank, 
      DATE_TRUNC(DATE_ADD(DATE(date_fin),INTERVAL 6 MONTH),YEAR) AS order_fiscal_year,
      DATE_DIFF(DATE_TRUNC(DATE(date_fin),YEAR),first_purchase_calendar_year,YEAR)+1 AS order_calendar_year_rank,
      DATE_TRUNC(DATE(date_fin),YEAR) AS order_calendar_year,
      DATE_DIFF(DATE_TRUNC(DATE(date_fin),MONTH),first_purchase_month,MONTH)+1 AS order_month_rank,
      DATE_TRUNC(DATE(date_fin),MONTH) AS order_month,
      first_purchase_calendar_year,
      first_purchase_fiscal_year,
      first_purchase_month,
      o.numero_tck_interne AS order_id,
      o.code_client AS customer_id,
      rev.ca_ttc,
      rev.ca_ht,
      'retail' AS scope
    FROM
      `amasty-data.prios.raw_prios_tickets` o
      LEFT JOIN new_customers nc USING (code_client)
      INNER JOIN prios_revenues rev USING (numero_tck_interne)
    WHERE
      nc.first_purchase_month >= DATE(2022,7,1)
      AND
      DATE_TRUNC(DATE(o.date_fin),MONTH) >= nc.first_purchase_month
      AND
      o.code_boutique_externe_vente != 'W0001'

    UNION ALL

    SELECT
      DATE_DIFF(DATE_TRUNC(DATE_ADD(DATE(date_fin),INTERVAL 6 MONTH),YEAR),first_purchase_fiscal_year,YEAR)+1 AS order_fiscal_year_rank, 
      DATE_TRUNC(DATE_ADD(DATE(date_fin),INTERVAL 6 MONTH),YEAR) AS order_fiscal_year,
      DATE_DIFF(DATE_TRUNC(DATE(date_fin),YEAR),first_purchase_calendar_year,YEAR)+1 AS order_calendar_year_rank,
      DATE_TRUNC(DATE(date_fin),YEAR) AS order_calendar_year,
      DATE_DIFF(DATE_TRUNC(DATE(date_fin),MONTH),first_purchase_month,MONTH)+1 AS order_month_rank,
      DATE_TRUNC(DATE(date_fin),MONTH) AS order_month,
      first_purchase_calendar_year,
      first_purchase_fiscal_year,
      first_purchase_month,
      o.numero_tck_interne AS order_id,
      o.code_client AS customer_id,
      rev.ca_ttc,
      rev.ca_ht,
      'omni' AS scope
    FROM
      `amasty-data.prios.raw_prios_tickets` o
      LEFT JOIN new_customers nc USING (code_client)
      INNER JOIN prios_revenues rev USING (numero_tck_interne)
    WHERE
      nc.first_purchase_month >= DATE(2022,7,1)
      AND
      DATE_TRUNC(DATE(o.date_fin),MONTH) >= nc.first_purchase_month 
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
      ROUND(SUM(ca_ttc),2) AS ca_ttc,
      ROUND(SUM(ca_ht),2) AS ca_ht,
      COUNT(DISTINCT order_id) AS orders_volume,
      COUNT(DISTINCT customer_id) AS unique_clients_volume,
      ROUND(SAFE_DIVIDE(SUM(ca_ttc),COUNT(DISTINCT order_id)),2) AS aov_ttc,
      ROUND(SAFE_DIVIDE(SUM(ca_ht),COUNT(DISTINCT order_id)),2) AS aov_ht,
      'web' AS scope
    FROM
      orders_cohorted
    WHERE
      scope = 'web'
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
      ROUND(SUM(ca_ttc),2) AS ca_ttc,
      ROUND(SUM(ca_ht),2) AS ca_ht,
      COUNT(DISTINCT order_id) AS orders_volume,
      COUNT(DISTINCT customer_id) AS unique_clients_volume,
      ROUND(SAFE_DIVIDE(SUM(ca_ttc),COUNT(DISTINCT order_id)),2) AS aov_ttc,
      ROUND(SAFE_DIVIDE(SUM(ca_ht),COUNT(DISTINCT order_id)),2) AS aov_ht,
      'omni' AS scope
    FROM
      orders_cohorted
    WHERE
      scope = 'omni'
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

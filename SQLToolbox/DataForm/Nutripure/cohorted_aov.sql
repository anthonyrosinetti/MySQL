WITH new_customers AS (
  SELECT DISTINCT
    id_customer,
    DATE_TRUNC(DATE_ADD(DATE(date_add),INTERVAL 3 MONTH),YEAR) AS first_purchase_year
    -- DATE_TRUNC(DATE(date_add),YEAR) AS first_purchase_year
    -- FIRST_VALUE(DATE(date_add)) OVER (PARTITION BY id_customer ORDER BY date_add) AS first_purchase_date
  FROM
    `cdp-boryl.prestashop.prestashop_raw_orders`
  WHERE
    # considérer uniquement les premiers achats 
    new_customer
    -- AND
    -- DATE_TRUNC(DATE(date_add),YEAR) >= DATE(2019,1,1)
),

non_cancelled_orders_cohorted AS (
  SELECT
    -- DATE_TRUNC(DATE(date_add),YEAR) AS order_year,
    DATE_DIFF(DATE_TRUNC(DATE_ADD(DATE(o.date_add),INTERVAL 3 MONTH),YEAR),first_purchase_year,YEAR)+1 AS order_year_rank,
    DATE_TRUNC(DATE_ADD(DATE(o.date_add),INTERVAL 3 MONTH),YEAR) AS order_year,
    o.id AS order_id,
    total_paid_tax_excl AS ca_ht,
    first_purchase_year
  FROM
    `cdp-boryl.prestashop.prestashop_raw_orders` o
    LEFT JOIN `cdp-boryl.prestashop.prestashop_raw_customers` c
    ON c.id = o.id_customer
    LEFT JOIN new_customers nc
    USING (id_customer)
  WHERE
    # exclusion des commandes annulées
    current_state NOT IN (6,46)
    AND
    nc.first_purchase_year IS NOT NULL
    # prise en compte des commandes B2C uniquement
    -- AND
    -- c.id_default_group IN ()
    AND
    c.siret IS NULL

    -- AND
    -- DATE_TRUNC(DATE(date_add), YEAR) >= DATE(2019,1,1)
)

SELECT
  order_year_rank,
  -- order_year,
  first_purchase_year,
  ROUND(SAFE_DIVIDE(SUM(ca_ht),COUNT(DISTINCT order_id)),2) AS aov
FROM
  non_cancelled_orders_cohorted
GROUP BY
  order_year_rank,
  first_purchase_year

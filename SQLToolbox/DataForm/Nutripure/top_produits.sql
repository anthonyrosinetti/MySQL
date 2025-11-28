WITH l1 AS (
  SELECT DISTINCT
    -- DATE_TRUNC(DATE_ADD(DATE(o.date_add),INTERVAL 3 MONTH),YEAR) AS order_year,
    DATE_TRUNC(DATE(o.date_add),MONTH) AS order_month,
    a.product_name,
    MAX(a.product_price) OVER (PARTITION BY DATE_TRUNC(DATE(o.date_add),MONTH),a.product_name) AS product_price,
    -- MAX(a.product_price) OVER (PARTITION BY a.product_name) AS product_price,
    -- SUM(a.product_quantity) OVER (PARTITION BY DATE_TRUNC(DATE_ADD(DATE(o.date_add),INTERVAL 3 MONTH),YEAR),a.product_name) AS year_product_quantity,
    SUM(a.product_quantity) OVER (PARTITION BY DATE_TRUNC(DATE(o.date_add),MONTH),a.product_name) AS month_product_quantity
  FROM
    `cdp-boryl.prestashop.prestashop_raw_orders` o,UNNEST(associations.order_rows) a
    LEFT JOIN `cdp-boryl.prestashop.prestashop_raw_customers` c ON o.id_customer = c.id
    LEFT JOIN `cdp-boryl.prestashop.prestashop_raw_products` p ON p.id = a.product_id
    LEFT JOIN `cdp-boryl.prestashop.prestashop_raw_product_categories` pc ON pc.id = p.id_category_default,UNNEST(pc.name) n
  WHERE
      # exclude canceled orders
      current_state NOT IN (6,46)
      AND
      # exclude refunded orders
      current_state NOT IN (7,34)      
      AND
      # keep B2C customers only
      (company IS NULL)
      AND
      (siret IS NULL)
      AND
      # keep clients only
      id_default_group = 3
      AND
      a.unit_price_tax_excl > 30
      AND
      DATE(o.date_add) BETWEEN DATE(2025,4,1) AND DATE(2025,9,30)
      -- EXTRACT(YEAR FROM DATE_TRUNC(DATE_ADD(DATE(o.date_add),INTERVAL 3 MONTH),YEAR)) IN (2022,2023)    
),

l2 AS (
  SELECT
    product_name,
    MAX(ROUND(CASE WHEN order_month BETWEEN DATE(2025,4,1) AND DATE(2025,6,1) THEN product_price ELSE NULL END,2)) AS product_price_q3,
    -- ROUND(product_price,2) AS product_price,
    SUM(CASE WHEN order_month BETWEEN DATE(2025,4,1) AND DATE(2025,6,1) THEN month_product_quantity ELSE 0 END) AS product_quantity_q3,
    MAX(ROUND(CASE WHEN order_month BETWEEN DATE(2025,7,1) AND DATE(2025,9,1) THEN product_price ELSE NULL END,2)) AS product_price_q4,
    SUM(CASE WHEN order_month BETWEEN DATE(2025,7,1) AND DATE(2025,9,1) THEN month_product_quantity ELSE 0 END) AS product_quantity_q4,    
    -- SUM(CASE WHEN EXTRACT(YEAR FROM order_year) = '2022' THEN year_product_quantity ELSE 0 END) AS product_quantity_2022,
    -- SUM(CASE WHEN EXTRACT(YEAR FROM order_year) = '2023' THEN year_product_quantity ELSE 0 END) AS product_quantity_2023
  FROM
    l1
  GROUP BY
    product_name
    -- product_price
    -- product_price_q3,
    -- product_price_q4
)

SELECT
  *,
  ROUND(100*SAFE_DIVIDE(product_price_q4-product_price_q3,product_price_q3),1) AS product_price_variation,
  ROUND(100*SAFE_DIVIDE(product_quantity_q4-product_quantity_q3,product_quantity_q3),1) AS product_quantity_variation
  -- ROUND(100*SAFE_DIVIDE(product_quantity_2023-product_quantity_2022,product_quantity_2022),1) AS product_quantity_variation
FROM
  l2
ORDER BY
  product_name

WITH layer1 AS (
  SELECT
  	  DATE_TRUNC(date,MONTH) AS month,
  	  store,
      manufacturer,
      surtype,
      type,
      suppliers,
      brand,
      product_sku,
      product_name,
      ROUND(SUM(item_total_revenue),0) AS monthly_ca,
      COUNT(DISTINCT order_id) AS monthly_orders,
      SUM(item_total_quantity) AS monthly_sold_units,
      AVG(item_margin) AS monthly_margin
  FROM
      `reporting-boryl-lcda.analysis.analysis_orders`
  WHERE
      date between PARSE_DATE('%Y%m%d',  @DS_START_DATE) AND PARSE_DATE('%Y%m%d',  @DS_END_DATE)
      and
      product_sku IN (
        SELECT
        sku
        FROM
        `reporting-boryl-lcda.config.filtered_sku`
      )	  
  GROUP BY
      DATE_TRUNC(date,MONTH),
      store,
      manufacturer,
      surtype,
      type,
      suppliers,
      brand,
      product_sku,
      product_name
)

SELECT
  *,
  DENSE_RANK() OVER (ORDER BY month) AS row_number_group
FROM
	layer1

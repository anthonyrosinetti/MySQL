CREATE OR REPLACE TABLE `cdp-boryl.aov_analysis.enriched_orders` AS (
  WITH product_types AS (
    SELECT DISTINCT
      p.id,
      CASE
        WHEN cat.id = 15 THEN 'pack'
        ELSE p.type
      END AS product_type  
    FROM
      `cdp-boryl.prestashop.prestashop_raw_products` p,UNNEST(associations.categories) cat
  ),

  order_product AS (
    SELECT DISTINCT
      o.id,
      CASE
        WHEN n.value IN (
          'Santé',
          'Mobilité et articulations',
          'Nutrition saine',
          'Stress et équilibre'
          ) THEN 'Santé'
        WHEN n.value IN (
          'Musculation',
          'Endurance'
          ) THEN 'Sport'
        ELSE 'Holistique'
      END AS product_category,
      pt.product_type,
      a.product_name,
      a.product_id,
      a.product_quantity,
      a.unit_price_tax_excl
    FROM
      `cdp-boryl.prestashop.prestashop_raw_orders` o,UNNEST(associations.order_rows) a
      LEFT JOIN `cdp-boryl.prestashop.prestashop_raw_customers` c ON o.id_customer = c.id    
      LEFT JOIN `cdp-boryl.prestashop.prestashop_raw_products` p ON p.id = a.product_id
      LEFT JOIN product_types pt ON pt.id = p.id
      LEFT JOIN `cdp-boryl.prestashop.prestashop_raw_product_categories` pc ON pc.id = p.id_category_default,UNNEST(pc.name) n
    WHERE
      current_state NOT IN (6,46)
      AND
      DATE(o.date_add) >= DATE(2022,10,1)
  ),

  intermediate_order_product_l1 AS (
    SELECT DISTINCT
      id,
      product_category,
      product_type,
      SUM(product_quantity) OVER (PARTITION BY id,product_category) AS order_category_occurrence,
      SUM(product_quantity) OVER (PARTITION BY id,product_type) AS order_type_occurrence,
      SUM(product_quantity) OVER (PARTITION BY id) AS items_count,
      AVG(unit_price_tax_excl) OVER (PARTITION BY id) AS average_item_price
    FROM
      order_product
  ),

  intermediate_order_product_l2 AS (
    SELECT DISTINCT
      *,
      MAX(order_category_occurrence) OVER (PARTITION BY id) AS order_max_category_occurrence,
      MAX(order_type_occurrence) OVER (PARTITION BY id) AS order_max_type_occurrence
    FROM
      intermediate_order_product_l1
  ),

  intermediate_order_product_l3 AS (
    SELECT DISTINCT
      *,
      CASE WHEN product_category = 'Holistique' AND order_category_occurrence = order_max_category_occurrence THEN True ELSE False END AS holistique_has_max_occurrence,
      CASE WHEN product_category = 'Sport' AND order_category_occurrence = order_max_category_occurrence THEN True ELSE False END AS sport_has_max_occurrence,
      CASE WHEN product_category = 'Santé' AND order_category_occurrence = order_max_category_occurrence THEN True ELSE False END AS sante_has_max_occurrence,
      CASE WHEN order_category_occurrence = order_max_category_occurrence THEN true ELSE false END AS product_category_has_max_order_occurrence,
      CASE WHEN product_type = 'simple' AND order_type_occurrence = order_max_type_occurrence THEN True ELSE False END AS simple_has_max_occurrence,
      CASE WHEN product_type = 'pack' AND order_type_occurrence = order_max_type_occurrence THEN True ELSE False END AS pack_has_max_occurrence,
      CASE WHEN product_type = 'virtual' AND order_type_occurrence = order_max_type_occurrence THEN True ELSE False END AS virtual_has_max_occurrence,
      CASE WHEN order_type_occurrence = order_max_type_occurrence THEN true ELSE false END AS product_type_has_max_order_occurrence
    FROM
      intermediate_order_product_l2
  ),

  -- intermediate_order_product_l4 AS (
  --   SELECT DISTINCT
  --     *,
  --     COUNT(DISTINCT CASE WHEN product_category_has_max_order_occurrence THEN product_category ELSE NULL END) OVER (PARTITION BY id) AS order_categories_having_max_categories,
  --     COUNT(DISTINCT CASE WHEN product_type_has_max_order_occurrence THEN product_type ELSE NULL END) OVER (PARTITION BY id) AS order_types_having_max_categories
  --   FROM
  --     intermediate_order_product_l3
  -- ),

  order_category_type AS (
    SELECT DISTINCT
      id,
      CASE
        WHEN MAX(sport_has_max_occurrence) AND MAX(sante_has_max_occurrence) = False THEN 'Sport'
        WHEN MAX(sante_has_max_occurrence) AND MAX(sport_has_max_occurrence) = False THEN 'Santé'
        ELSE 'Holistique'
      END AS persona,
      -- CASE WHEN MAX(order_categories_having_max_categories) > 1 THEN 'Holistique' ELSE ANY_VALUE(product_category HAVING MAX order_category_occurrence) END AS persona,
      CASE
        WHEN MAX(simple_has_max_occurrence) AND MAX(pack_has_max_occurrence) = False THEN 'Simple'
        WHEN MAX(pack_has_max_occurrence) AND MAX(simple_has_max_occurrence) = False THEN 'Pack'
        WHEN MAX(pack_has_max_occurrence) AND MAX(simple_has_max_occurrence) THEN 'Pack'
        ELSE 'Virtual'
      END AS main_type,
      -- CASE WHEN MAX(order_types_having_max_categories) > 1 THEN 'pack' ELSE ANY_VALUE(product_type HAVING MAX order_type_occurrence) END AS main_type,
      MAX(items_count) AS items_count,
      ROUND(AVG(average_item_price),2) AS average_item_price
    FROM
      intermediate_order_product_l3
    GROUP BY
      id
  ),

  customers AS (
    SELECT DISTINCT 
      id,
      CASE
        WHEN (
              (company IS NULL)
              AND
              (siret IS NULL)
              AND
              (
                id_default_group NOT IN (
                4, #B2B Magasins
                5, #B2B Pharmacies
                7, #B2B Salles
                9, #B2B Prescripteurs
                11, #B2B Clubs et Associations
                12, #B2B Revendeurs indépendants
                13, #Revendeurs Contrat standard
                15, #Revendeurs Contrat Fidélité
                37, #B2B Nutritionnistes et Diets
                45, #B2B Prélèvement automatique
                46, #B2B Contrat Intersport
                60, #B2B Naturopathes
                61, #B2B Coach Sportifs
                62, #B2B Kinésithérapeute
                71  #B2B Contrat BodyHit    
                )
              )
        ) THEN 'B2C'
          ELSE 'B2B'
      END AS customer_type,
      CASE
        WHEN CAST(LEFT(birthday,4) AS INT64) BETWEEN 1945 AND 2010 THEN
  DATE_DIFF(CURRENT_DATE(), DATE(birthday), YEAR) - IF(FORMAT_DATE('%m-%d', CURRENT_DATE()) < FORMAT_DATE('%m-%d', DATE(birthday)), 1, 0)
        ELSE CAST(NULL AS INT64)
      END AS age,
      CASE id_gender 
        WHEN 1 THEN 'Homme'
        WHEN 2 THEN 'Femme'
        ELSE NULL
      END AS gender,
      id_default_group
    FROM
      `cdp-boryl.prestashop.prestashop_raw_customers`
  ),

  addresses_countries AS (
    SELECT DISTINCT
      a.id AS id_address_invoice,
      FIRST_VALUE(n.value) OVER (PARTITION BY c.id ORDER BY n.id ASC) AS country
    FROM
      `cdp-boryl.prestashop.prestashop_raw_addresses` a
      LEFT JOIN
      `cdp-boryl.prestashop.prestashop_raw_countries` c ON c.id = a.id_country,UNNEST(c.name) n
  ),

  customers_first_order_dates AS (
    SELECT DISTINCT
      id_customer,
      MIN(DATE(date_add)) AS first_order_date
    FROM
      `cdp-boryl.prestashop.prestashop_raw_orders`
    GROUP BY
      id_customer
  )

  SELECT DISTINCT
    DATE(o.date_add) AS date,
    CASE EXTRACT(DAYOFWEEK FROM o.date_add)
      WHEN 1 THEN 'Dimanche'
      WHEN 2 THEN 'Lundi'
      WHEN 3 THEN 'Mardi'
      WHEN 4 THEN 'Mercredi'
      WHEN 5 THEN 'Jeudi'
      WHEN 6 THEN 'Vendredi'
      WHEN 7 THEN 'Samedi'
    END AS weekday,
    CASE
      WHEN EXTRACT(HOUR FROM o.date_add) BETWEEN 0 AND 5 THEN 'Nuit'
      WHEN EXTRACT(HOUR FROM o.date_add) BETWEEN 6 AND 11 THEN 'Matin'
      WHEN EXTRACT(HOUR FROM o.date_add) BETWEEN 12 AND 13 THEN 'Midi'
      WHEN EXTRACT(HOUR FROM o.date_add) BETWEEN 14 AND 17 THEN 'Après-midi'
      ELSE 'Soir'
    END AS day_period,
    o.id_customer,
    new_customer,
    fo.first_order_date,
    DATE_DIFF(DATE(o.date_add), fo.first_order_date, MONTH) AS seniority,
    CASE
      WHEN c.age < 20 THEN '[-20 ans]'
      WHEN c.age BETWEEN 20 AND 34 THEN '[20 ans - 35 ans]'
      WHEN c.age BETWEEN 35 AND 49 THEN '[35 ans - 50 ans]'
      WHEN c.age >= 50 THEN '[+50 ans]'
    ELSE '-'
    END AS age_range,
    c.gender,
    c.customer_type,
    c.id_default_group,
    o.id AS id_order,
    o.current_state,
    a.country,
    ROW_NUMBER() OVER (PARTITION BY o.id_customer ORDER BY DATE(o.date_add) ASC) AS order_rank,
    CASE
      WHEN total_discounts_tax_excl = 0 THEN false
      WHEN total_discounts_tax_excl > 0 THEN true
    ELSE NULL
    END AS with_discount,
    CASE
      WHEN total_shipping_tax_excl = 0 THEN true
      WHEN total_shipping_tax_excl > 0 THEN false
    ELSE NULL
    END AS free_shipping,
    oc.persona,
    oc.main_type,
    oc.items_count,
    oc.average_item_price,
    total_paid_tax_excl AS ca_ht,
    ROUND(100*SAFE_DIVIDE(total_discounts_tax_excl,total_discounts_tax_excl+total_paid_tax_excl),1) AS promotion_rate
  FROM
    `cdp-boryl.prestashop.prestashop_raw_orders` o
    LEFT JOIN
    customers_first_order_dates fo
    ON
      fo.id_customer = o.id_customer
    LEFT JOIN
    customers c ON o.id_customer = c.id
    LEFT JOIN
    order_category_type oc ON oc.id = o.id
    LEFT JOIN
    addresses_countries a USING (id_address_invoice)
  WHERE
    current_state NOT IN (6,46)
    AND
    DATE(o.date_add) >= DATE(2022,10,1)  
)

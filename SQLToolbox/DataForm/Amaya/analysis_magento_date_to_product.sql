
-- Config block
config {
  type: "table",
  bigquery: {
    partitionBy: "date"
  },  
  schema: "analysis"
}

-- SQL
with magento_customers as (
    select distinct
      customer_id,
      c.email,
      case c.gender
        when 0 then '-'
        when 1 then 'Male'
        when 2 then 'Male'
        when 3 then '-'
        when 5 then 'Female'
        when 8 then 'Female'
        else '-'
      end as gender,
      case	
        when date_diff(current_date(), date(dob), YEAR) - 
        if(format_date('%m-%d', current_date()) < format_date('%m-%d', date(dob)), 1, 0) < 20 then '[-20 ans]'
        when date_diff(current_date(), date(dob), YEAR) - 
        if(format_date('%m-%d', current_date()) < format_date('%m-%d', date(dob)), 1, 0) < 35 then '[20 ans - 35 ans]'
        when date_diff(current_date(), date(dob), YEAR) - 
        if(format_date('%m-%d', current_date()) < format_date('%m-%d', date(dob)), 1, 0) < 50 then '[35 ans - 50 ans]'
        when date_diff(current_date(), date(dob), YEAR) - 
        if(format_date('%m-%d', current_date()) < format_date('%m-%d', date(dob)), 1, 0) >= 50 then '[+50 ans]'
      else '-'
      end as age_range
  from ${ref("raw_magento_customers")} c,unnest(addresses) adress
),

customers_first_order_dates as (
    select distinct
        customer_id,
        date(
            datetime_add(datetime(min(created_at) over (partition by customer_id)),interval 2 hour)
        ) as customer_first_order_date
    from
        ${ref("raw_magento_orders")}
),

country_mapping_codes AS (
    SELECT DISTINCT
        alpha2_code,
        FIRST_VALUE(french_name) OVER (
            PARTITION BY alpha2_code
            ORDER BY id ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS country_name
    FROM
        ${ref("config_countries")}
),

magento_products as (
  select distinct
      id as product_id,
      sku as product_sku,
      name as product_name,
      type_id as product_type,
      case when status = 1
      and visibility = 4 then true else false end as is_product_active,
      (
          select
              json_extract_scalar(c, '$.value')
          from
              unnest(json_extract_array(custom_attributes)) c
          where
              json_extract_scalar(c, '$.attribute_code') = 'reference_matiere'
      ) as matiere_id,
      (
          select
              json_extract_scalar(c, '$.value')
          from
              unnest(json_extract_array(custom_attributes)) c
          where
              json_extract_scalar(c, '$.attribute_code') = 'reference_famille_magento'
      ) as famille_id,
      (
          select
              json_extract_scalar(c, '$.value')
          from
              unnest(json_extract_array(custom_attributes)) c
          where
              json_extract_scalar(c, '$.attribute_code') = 'reference_ss_famille_magento'
      ) as ss_famille_id,
      (
          select
              json_extract_scalar(c, '$.value')
          from
              unnest(json_extract_array(custom_attributes)) c
          where
              json_extract_scalar(c, '$.attribute_code') = 'reference_collection_enrich'
      ) as collection_id,
      (
          select
              json_extract_scalar(c, '$.value')
          from
              unnest(json_extract_array(custom_attributes)) c
          where
              json_extract_scalar(c, '$.attribute_code') = 'label_is_new'
      ) as is_new,
      (
          select
              json_extract_scalar(c, '$.value')
          from
              unnest(json_extract_array(custom_attributes)) c
          where
              json_extract_scalar(c, '$.attribute_code') = 'bundle_type'
      ) as bundle_type,
      (
          select
              json_extract_scalar(c, '$.value')
          from
              unnest(json_extract_array(custom_attributes)) c
          where
              json_extract_scalar(c, '$.attribute_code') = 'reference_famille_commerciale'
      ) as famille_commerciale_id,
      (
          select
              json_extract_scalar(c, '$.value')
          from
              unnest(json_extract_array(custom_attributes)) c
          where
              json_extract_scalar(c, '$.attribute_code') = 'reference_gravable'
      ) as is_gravable
  from
      ${ref("raw_magento_products")}
),

matched_product_attributes as (
    select distinct
        product_id,
        product_sku,
        product_name,
        product_type,
        is_product_active,
        matiere.label as matiere,
        famille.label as famille,
        ss_famille.label as ss_famille,
        collection.label as collection,
        case is_new when '1' then 'Oui' when '0' then 'Non' end as is_new,
        famille_commerciale.label as famille_commerciale,
        gravable.label as is_gravable,
        bundle_type.label as bundle_type
    from
        magento_products m
        left join (
            select
                o.label,
                o.value
            from
                ${ref("raw_magento_product_attributes")},
                unnest(options) as o
            where
                attribute_code = 'reference_matiere'
        ) matiere on m.matiere_id = matiere.value
        left join (
            select
                o.label,
                o.value
            from
                ${ref("raw_magento_product_attributes")},
                unnest(options) as o
            where
                attribute_code = 'reference_famille_magento'
        ) famille on m.famille_id = famille.value
        left join (
            select
                o.label,
                o.value
            from
                ${ref("raw_magento_product_attributes")},
                unnest(options) as o
            where
                attribute_code = 'reference_ss_famille_magento'
        ) ss_famille on m.ss_famille_id = ss_famille.value
        left join (
            select
                o.label,
                o.value
            from
                ${ref("raw_magento_product_attributes")},
                unnest(options) as o
            where
                attribute_code = 'reference_collection_enrich'
        ) collection on m.collection_id = collection.value
        left join (
            select
                o.label,
                o.value
            from
                ${ref("raw_magento_product_attributes")},
                unnest(options) as o
            where
                attribute_code = 'reference_famille_commerciale'
        ) famille_commerciale on m.famille_commerciale_id = famille_commerciale.value
        left join (
            select
                o.label,
                o.value
            from
                ${ref("raw_magento_product_attributes")},
                unnest(options) as o
            where
                attribute_code = 'bundle_type'
        ) bundle_type on m.bundle_type = bundle_type.value
        left join (
            select
                o.label,
                o.value
            from
                ${ref("raw_magento_product_attributes")},
                unnest(options) as o
            where
                attribute_code = 'reference_gravable'
        ) gravable on m.is_gravable = gravable.value        
),

product_options as (
  select
    p.sku,
    p.type_id as product_type,
    cast(o.option_id as string) as option_id,
    o.type
  from
    ${ref("raw_magento_products")} p,
    unnest(options) o
),

orders_with_options as (
  select distinct
    increment_id,
    i.sku,
    product_id,
    c.option_id,
    c.option_value
  from
    ${ref("raw_magento_orders")} o,
    unnest(items) i,
    unnest(i.product_option.extension_attributes.custom_options) c
  --where
    --i.product_id != 5 -- Carte cadeau
),

orders_with_graved_products as (
    select
        increment_id,
        case
            when po.product_type = 'simple' then o.sku
            when po.product_type = 'configurable' then pr.product_sku
            when po.product_type = 'bundle' then regexp_extract(o.sku, r'^([A-Z0-9]+)')
            else pr.product_sku
        end as product_sku,
        max(case when option_value is not null and type = 'field' then true else false end) as product_word_engraved,
        max(case when option_value is not null and type  = 'symbol' then true else false end) as product_symbol_engraved
    from
        orders_with_options o
        left join product_options po using (option_id)
        left join matched_product_attributes pr using (product_id)
    group by
        increment_id,
        product_sku  
),

all_orders as (
    select
        date(datetime_add(datetime(o.created_at),interval 2 hour)) as date,
        increment_id as order_id,
        o.store_id,
        cmc.country_name as billing_country,
        status,
        cast(o.customer_id as string) as customer_id,
        customer_email as email,
        ifnull(c.gender,'-') as gender,
        c.age_range,
        cfo.customer_first_order_date,
        cast(i.product_id as string) as product_id,
        case
            when pr.product_type = 'simple' then i.sku
            when pr.product_type = 'configurable' then pr.product_sku
            when pr.product_type = 'bundle' then regexp_extract(i.sku, r'^([A-Z0-9]+)')
            else pr.product_sku
        end as product_sku,
        pr.product_name,
        pr.product_type,
        pr.is_new,
        pr.is_gravable,
        ifnull(ogp.product_word_engraved,false) as product_word_engraved,
        ifnull(ogp.product_symbol_engraved,false) as product_symbol_engraved,     
        ifnull(pr.matiere,'-') as matiere,
        ifnull(case
            when lower(pr.ss_famille) like '%composition%' then 'Compositions'
            when pr.product_type = 'amgiftcard' then 'Cartes cadeau'
            else pr.famille
        end,'-') as famille,
        ifnull(pr.ss_famille,'-') as ss_famille,
        case
          when pr.famille_commerciale is null and pr.famille != 'Bases' then 'Femme'
          else ifnull(pr.famille_commerciale,'-')
        end as famille_commerciale,
        ifnull(pr.collection,'-') as collection,
        case
            when pr.product_type in ('simple','configurable') then 'Produit fini'
            when pr.bundle_type = 'composition' then 'Composition'
            when pr.bundle_type = 'montage' then 'Montage'
        end as bundle_type,
        cast(ifnull(i.qty_ordered, 0) as int) as item_total_quantity,
        round(ifnull(i.base_price_incl_tax, 0) * ifnull(i.qty_ordered, 0),2) as item_total_revenue_ttc,
        round(ifnull(i.base_price, 0) * ifnull(i.qty_ordered, 0),2) as item_total_revenue_ht
    from
        ${ref("raw_magento_orders")} o,
        unnest(items) i
        left join matched_product_attributes pr using (product_id)
        left join orders_with_graved_products ogp using (increment_id,product_sku)
        left join magento_customers c using (customer_id)
        left join customers_first_order_dates cfo using (customer_id)
        left join country_mapping_codes cmc on o.billing_address.
country_id = alpha2_code
  --where
    --i.product_id != 5 -- Carte cadeau
),

engraved_orders as (
    select
        *,
        max(product_word_engraved) over (partition by order_id) as order_word_engraved,
        max(product_symbol_engraved) over (partition by order_id) as order_symbol_engraved
    from
        all_orders
)

select
    *,
    case
        when customer_first_order_date = date then true
        when customer_first_order_date < date then false
    end as bought_by_new
from
    engraved_orders 

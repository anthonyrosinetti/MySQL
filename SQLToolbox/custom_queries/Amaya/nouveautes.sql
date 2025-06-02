with magento_products as (
  select distinct
      id as product_id,
      sku as product_sku,
      name as product_name,
      type_id as product_type,
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
      ) as famille_commerciale_id
  from
      `amasty-data.magento.raw_magento_products`
),

matched_product_attributes as (
    select distinct
        cast(product_id as int64) as product_id,
        product_sku,
        product_name,
        product_type,
        matiere.label as matiere,
        famille.label as famille,
        ss_famille.label as ss_famille,
        collection.label as collection,
        case is_new when '1' then 'Oui' when '0' then 'Non' end as is_new,
        famille_commerciale.label as famille_commerciale,
        bundle_type.label as bundle_type
    from
        magento_products m
        left join (
            select
                o.label,
                o.value
            from
                `amasty-data.magento.raw_magento_product_attributes`,
                unnest(options) as o
            where
                attribute_code = 'reference_matiere'
        ) matiere on m.matiere_id = matiere.value
        left join (
            select
                o.label,
                o.value
            from
                `amasty-data.magento.raw_magento_product_attributes`,
                unnest(options) as o
            where
                attribute_code = 'reference_famille_magento'
        ) famille on m.famille_id = famille.value
        left join (
            select
                o.label,
                o.value
            from
                `amasty-data.magento.raw_magento_product_attributes`,
                unnest(options) as o
            where
                attribute_code = 'reference_ss_famille_magento'
        ) ss_famille on m.ss_famille_id = ss_famille.value
        left join (
            select
                o.label,
                o.value
            from
                `amasty-data.magento.raw_magento_product_attributes`,
                unnest(options) as o
            where
                attribute_code = 'reference_collection_enrich'
        ) collection on m.collection_id = collection.value
        left join (
            select
                o.label,
                o.value
            from
                `amasty-data.magento.raw_magento_product_attributes`,
                unnest(options) as o
            where
                attribute_code = 'reference_famille_commerciale'
        ) famille_commerciale on m.famille_commerciale_id = famille_commerciale.value
        left join (
            select
                o.label,
                o.value
            from
                `amasty-data.magento.raw_magento_product_attributes`,
                unnest(options) as o
            where
                attribute_code = 'bundle_type'
        ) bundle_type on m.bundle_type = bundle_type.value       
),

new_products_bought_current as (
  select  
    p.*,
    case when date >= date(datetime(h1._ingestion_timestamp)) and date <= date(datetime(h2._ingestion_timestamp)) then true else false end as new_product_bought
  from
    `amasty-data.analysis.analysis_magento_date_to_product` p
    left join
    `amasty-data.analysis.products_new_label_history` h1
    on cast(p.product_id as int64) = h1.product_id and h1.is_new_label
    left join
    `amasty-data.analysis.products_new_label_history` h2
    on cast(p.product_id as int64) = h2.product_id and h2.is_new_label = False
  where
	  date between PARSE_DATE('%Y%m%d',  @DS_START_DATE) AND PARSE_DATE('%Y%m%d',  @DS_END_DATE)
    and
    item_total_revenue_ttc != 0
),

union_new_global as (
  select
    date,
    product_sku,
    '' as billing_country,
	'' as status,
    '' as gender,
    cast(product_id as string) as product_id,
    '' as product_name,
    product_type,
    is_new,
    matiere,
    famille,
    ss_famille,
    famille_commerciale,
    collection,
    bundle_type,
    0 as item_total_quantity,
    0 as item_total_revenue_ttc,
    0 as item_total_revenue_ht,
    count(distinct h.product_id) as created_skus,
    'new_current_created' as scope
  from
    unnest(
      generate_date_array(
        PARSE_DATE('%Y%m%d',  @DS_START_DATE),
        PARSE_DATE('%Y%m%d',  @DS_END_DATE)
      )
    ) date
    left join
    `amasty-data.analysis.products_new_label_history` h
    on date = date(datetime(h._ingestion_timestamp)) and is_new_label
    left join matched_product_attributes using (product_id)
    group by 
      date, product_sku, billing_country, product_id, product_type, is_new, matiere, famille, ss_famille, famille_commerciale, collection, bundle_type, item_total_quantity, item_total_revenue_ttc, item_total_revenue_ht
  
    union all

  select
    date,
    np.product_sku,
    np.billing_country,
    np.status,
  	np.gender,
    np.product_id,
    np.product_name,
    np.product_type,
    np.is_new,
    np.matiere,
    np.famille,
    np.ss_famille,
    np.famille_commerciale,    
    np.collection,
    np.bundle_type,
    np.item_total_quantity,  
    np.item_total_revenue_ttc,
    np.item_total_revenue_ht,
    0 as created_skus,
    'new_current_bought' as scope
  from
    unnest(
      generate_date_array(
        PARSE_DATE('%Y%m%d',  @DS_START_DATE),
        PARSE_DATE('%Y%m%d',  @DS_END_DATE)
      )
    ) date
    left join
        (
        select
          *
        from
          new_products_bought_current
        where
          new_product_bought
        ) np using (date)

  union all

  select
    date,
    np.product_sku,
    np.billing_country,
    np.status,
  	np.gender,  
    np.product_id,
    np.product_name,
    np.product_type,
    np.is_new,
    np.matiere,
    np.famille,
    np.ss_famille,
    np.famille_commerciale,    
    np.collection,
    np.bundle_type,
    np.item_total_quantity,  
    np.item_total_revenue_ttc,
    np.item_total_revenue_ht,
    0 as created_skus,
    'global_current_bought' as scope
  from
    unnest(
      generate_date_array(
        PARSE_DATE('%Y%m%d',  @DS_START_DATE),
        PARSE_DATE('%Y%m%d',  @DS_END_DATE)
      )
    ) date
    left join
        (
            select  
                *
            from
                `amasty-data.analysis.analysis_magento_date_to_product`
            where
                date between PARSE_DATE('%Y%m%d',  @DS_START_DATE) AND PARSE_DATE('%Y%m%d',  @DS_END_DATE)
                and
                item_total_revenue_ttc != 0
        ) np using (date)
)

select
  date,
  billing_country,
  status,
  gender,
  product_sku,
  product_name,
  matiere,
  famille,
  ss_famille,
  famille_commerciale,
  collection,
  bundle_type,
  0 as current_new_unites_vendues,
  0 as current_new_ca_ttc,
  0 as current_new_ca_ht,
  max(created_skus) as current_created_skus,
  0 as current_global_unites_vendues,
  0 as current_global_ca_ttc,
  0 as current_global_ca_ht,
  scope
from
  union_new_global
where
  scope = 'new_current_created'
group by
  date,
  billing_country,
  status,
  gender,
  product_sku,
  product_name,
  matiere,
  famille,
  ss_famille,
  famille_commerciale,
  collection,
  bundle_type,
  scope

union all

select
  date,
  billing_country,
  status,
  gender,
  product_sku,
  product_name,
  matiere,
  famille,
  ss_famille,
  famille_commerciale,
  collection,
  bundle_type,
  sum(item_total_quantity) as current_new_unites_vendues,
  sum(item_total_revenue_ttc) as current_new_ca_ttc,
  sum(item_total_revenue_ht) as current_new_ca_ht,
  0 as current_created_skus,
  0 as current_global_unites_vendues,
  0 as current_global_ca_ttc,
  0 as current_global_ca_ht,
  scope
from
  union_new_global
where
  scope = 'new_current_bought'
group by
  date,
  billing_country,
  status,
  gender,
  product_sku,
  product_name,
  matiere,
  famille,
  ss_famille,
  famille_commerciale,
  collection,
  bundle_type,
  scope

union all

select
  date,
  billing_country,
  status,
  gender,
  product_sku,
  product_name,
  matiere,
  famille,
  ss_famille,
  famille_commerciale,
  collection,
  bundle_type,
  0 as current_new_unites_vendues,
  0 as current_new_ca_ttc,
  0 as current_new_ca_ht,
  0 as current_created_skus,
  sum(item_total_quantity) as current_global_unites_vendues,
  sum(item_total_revenue_ttc) as current_global_ca_ttc,
  sum(item_total_revenue_ht) as current_global_ca_ht,
  scope
from
  union_new_global
where
  scope = 'global_current_bought'
group by
  date,
  billing_country,
  status,
  gender,
  product_sku,
  product_name,
  matiere,
  famille,
  ss_famille,
  famille_commerciale,
  collection,
  bundle_type,
  scope

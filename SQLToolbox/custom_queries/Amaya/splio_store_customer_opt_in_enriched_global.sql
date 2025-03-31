with enriched_global_opt_ins as (
  select
    date(datetime(_ingestion_timestamp)) as _ingestion_timestamp,
    id,
    case when cf1_optin_email = 1 then true else false end as current_cf1_optin_email,
    lag(case when cf1_optin_email = 1 then true else false end) over w as previous_cf1_optin_email,
    case when cf2_optin_phone = 1 then true else false end as current_cf2_optin_phone,
    lag(case when cf2_optin_phone = 1 then true else false end) over w as previous_cf2_optin_phone,
    case when (safe.parse_date('%Y-%m-%d', cf15_birthdate) is not null and left(cf15_birthdate,4) != '1900') or (safe.parse_date('%d/%m/%Y', cf15_birthdate) is not null and right(cf15_birthdate,4) != '1900') then true else false end as current_cf15_birthdate,
    lag(case when (safe.parse_date('%Y-%m-%d', cf15_birthdate) is not null and left(cf15_birthdate,4) != '1900') or (safe.parse_date('%d/%m/%Y', cf15_birthdate) is not null and right(cf15_birthdate,4) != '1900') then true else false end) over w as previous_cf15_birthdate
  from
    `amasty-data.splio.raw_splio_contacts_history`
  window w as (
    partition by id
    order by date(datetime(_ingestion_timestamp)) asc
  )
),

opt_ins_switches as (
  select
    _ingestion_timestamp,
    id,
    previous_cf1_optin_email,
    case when current_cf1_optin_email and (previous_cf1_optin_email = False or previous_cf1_optin_email is null) then true else false end as is_email_opt_in_switch,
    previous_cf2_optin_phone,
    case when current_cf2_optin_phone and (previous_cf2_optin_phone = False or previous_cf2_optin_phone is null) then true else false end as is_phone_opt_in_switch,
    previous_cf15_birthdate,
    case when current_cf15_birthdate and (previous_cf15_birthdate = False or previous_cf15_birthdate is null) then true else false end as is_birthday_opt_in_switch       
  from
    enriched_global_opt_ins
),

enriched_purchases_opt_ins_switches as (
select distinct
  o.customer_id,
  o.store,
  o.order_created_date,
  first_value(os.previous_cf1_optin_email) over w as previous_cf1_optin_email,
  first_value(os.is_email_opt_in_switch) over w as is_email_opt_in_switch,
  first_value(os.previous_cf2_optin_phone) over w as previous_cf2_optin_phone,
  first_value(os.is_phone_opt_in_switch) over w as is_phone_opt_in_switch,
  first_value(os.previous_cf15_birthdate) over w as previous_cf15_birthdate,
  first_value(os.is_birthday_opt_in_switch) over w as is_birthday_opt_in_switch,
from
  `amasty-data.analysis.preanalysis_orders_splio` o
  left join
  opt_ins_switches os
      on cast(os.id as string) = o.customer_id and o.order_created_date <= os._ingestion_timestamp
where
  o.order_created_date between greatest(PARSE_DATE("%Y%m%d", @DS_START_DATE),date(2025,2,5)) and PARSE_DATE("%Y%m%d", @DS_END_DATE)
window w as (
    partition by customer_id,store,order_created_date
    order by date(datetime(_ingestion_timestamp)) asc
	ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  )
),

enriched_customers_opt_ins as (
select
  customer_id,
  store,
  order_created_date,
  case when min(ifnull(previous_cf1_optin_email,False)) over (partition by customer_id) = False then True else False end as customer_had_order_being_email_optout,
  min(case when is_email_opt_in_switch then order_created_date else null end) over (partition by customer_id) as first_email_opt_in_order_date,
  case when min(ifnull(previous_cf2_optin_phone,False)) over (partition by customer_id) != True then True else False end as customer_had_order_being_phone_optout,
  min(case when is_phone_opt_in_switch then order_created_date else null end) over (partition by customer_id) as first_phone_opt_in_order_date,  
  case when min(ifnull(previous_cf15_birthdate,False)) over (partition by customer_id) != True then True else False end as customer_had_order_being_birthday_optout,
  min(case when is_birthday_opt_in_switch then order_created_date else null end) over (partition by customer_id) as first_birthday_opt_in_order_date, 
from
  enriched_purchases_opt_ins_switches
order by 3 asc
)

select
  customer_id,
  store,
  max(case when order_created_date = first_email_opt_in_order_date then store else null end) over (partition by customer_id) as first_email_opt_in_switch_store,
  max(customer_had_order_being_email_optout) over (partition by customer_id) as customer_had_order_being_email_optout,
  min(first_email_opt_in_order_date) over (partition by customer_id) as first_email_opt_in_order_date,
  max(case when order_created_date = first_phone_opt_in_order_date then store else null end) over (partition by customer_id) as first_phone_opt_in_switch_store,
  max(customer_had_order_being_phone_optout) over (partition by customer_id) as customer_had_order_being_phone_optout,
  min(first_phone_opt_in_order_date) over (partition by customer_id) as first_phone_opt_in_order_date,
  max(case when order_created_date = first_birthday_opt_in_order_date then store else null end) over (partition by customer_id) as first_birthday_opt_in_switch_store,
  max(customer_had_order_being_birthday_optout) over (partition by customer_id) as customer_had_order_being_birthday_optout,
  min(first_birthday_opt_in_order_date) over (partition by customer_id) as first_birthday_opt_in_order_date
from
  enriched_customers_opt_ins

with room_last_avail as (
  select distinct
    r.id,
    last_value(date(a.availability_dates.created_at)) over w as room_last_availability_creation_date,
  from
    `big-query-328314.postgres.postgres_raw_rooms` r
    left join
      `big-query-328314.db_data.preanalysis_db_availabilities_requests` a
    on r.id = a.room_id
  where
  	date(a.availability_dates.created_at) between parse_date("%Y%m%d", @DS_START_DATE) and parse_date("%Y%m%d", @DS_END_DATE)
  window w as (
    partition by a.room_id
    order by a.availability_dates.created_at asc
    rows between unbounded preceding and unbounded following    
  )
)
, reduced_properties as (
    select
        p.id as property_id,
        r.id as room_id,
        min(rla.room_last_availability_creation_date) over (partition by property_id) as property_earliest_room_availability_creation_date,
        max(rla.room_last_availability_creation_date) over (partition by property_id) as property_latest_room_availability_creation_date,
        r.rent_without_expenses_amount+ifnull(r.expenses_amount,0) as rent,
        p.rooms_count,
        p.property_surface,
        r.surface,
        p.property_type,
        b.city,
        ifnull(p.furnished, false) as furnished,
        p.rented_by_room,
        p.fully_rentable
    from
        `big-query-328314.postgres.postgres_raw_properties` p
        left join `big-query-328314.postgres.postgres_raw_rooms` r on r.property_id = p.id
        left join room_last_avail rla on rla.id = r.id
        left join `big-query-328314.postgres.postgres_raw_buildings` b on b.id = p.building_id
    where
        /*
        add moderation
        */
        p.status in ('offline', 'online')
        and p.announcement_type in ('flat_share', 'rental', 'sublet')
        and p.property_surface > r.surface
        and r.rent_without_expenses_amount > 300
)
, filtered_outliers as (
  select
    percentile_cont(rent, 0.99 ignore nulls) over() as q3_rent
  from
    reduced_properties
  limit 1
)
, room_scope as (
    select
        *,
        case when rented_by_room and fully_rentable and furnished and property_surface > 0 then round(safe_divide(sum(rent) over w, property_surface),2) else null end as prix_au_m2_coloc_meuble,
        case when rented_by_room and fully_rentable and furnished = false and property_surface > 0 then round(safe_divide(sum(rent) over w, property_surface),2) else null end as prix_au_m2_coloc_non_meuble,
        case when rented_by_room = false and furnished and (property_type = 'studio' or rooms_count in (0, 1)) and property_surface > 0 then round(safe_divide(sum(rent) over w,property_surface),2) else null end as prix_au_m2_studio_meuble,
        case when rented_by_room = false and furnished = false and (property_type = 'studio' or rooms_count in (0, 1)) and property_surface > 0 then round(safe_divide(sum(rent) over w,property_surface),2) else null end as prix_au_m2_studio_non_meuble,
        case when rented_by_room = false and furnished and property_type != 'studio' and rooms_count = 2 and property_surface > 0 then round(safe_divide(sum(rent) over w,property_surface),2) else null end as prix_au_m2_t2_meuble,
        case when rented_by_room = false and furnished = false and property_type != 'studio' and rooms_count = 2 and property_surface > 0 then round(safe_divide(sum(rent) over w, property_surface),2) else null end as prix_au_m2_t2_non_meuble,
        case when rented_by_room = false and furnished and property_type != 'studio' and rooms_count = 3 and property_surface > 0 then round(safe_divide(sum(rent) over w,property_surface),2) else null end as prix_au_m2_t3_meuble,
        case when rented_by_room = false and furnished = false and property_type != 'studio' and rooms_count = 3 and property_surface > 0 then round(safe_divide(sum(rent) over w,property_surface),2) else null end as prix_au_m2_t3_non_meuble,
        case when rented_by_room = false and furnished and property_type != 'studio' and rooms_count >= 4 and property_surface > 0 then round(safe_divide(sum(rent) over w,property_surface),2) else null end as prix_au_m2_t4_plus_meuble,
        case when rented_by_room = false and furnished = false and property_type != 'studio' and rooms_count >= 4 and property_surface > 0 then round(safe_divide(sum(rent) over w,property_surface),2) else null end as prix_au_m2_t4_plus_non_meuble
    from
        reduced_properties rp
        join
          filtered_outliers o
            on
              rp.rent < o.q3_rent
    where
        date_trunc(property_latest_room_availability_creation_date,year) >= date(2024,1,1)
        and
        date_diff(property_latest_room_availability_creation_date,property_earliest_room_availability_creation_date,month) <= 6
    window w as (
            partition by property_id
        )
)
, property_scope as (
  select
    city,
    property_id,
    max(prix_au_m2_coloc_meuble) as prix_au_m2_coloc_meuble,
    max(prix_au_m2_coloc_non_meuble) as prix_au_m2_coloc_non_meuble,
    max(prix_au_m2_studio_meuble) as prix_au_m2_studio_meuble,
    max(prix_au_m2_studio_non_meuble) as prix_au_m2_studio_non_meuble,
    max(prix_au_m2_t2_meuble) as prix_au_m2_t2_meuble,
    max(prix_au_m2_t2_non_meuble) as prix_au_m2_t2_non_meuble,
    max(prix_au_m2_t3_meuble) as prix_au_m2_t3_meuble,
    max(prix_au_m2_t3_non_meuble) as prix_au_m2_t3_non_meuble,
    max(prix_au_m2_t4_plus_meuble) as prix_au_m2_t4_plus_meuble,
    max(prix_au_m2_t4_plus_non_meuble) as prix_au_m2_t4_plus_non_meuble
  from
    room_scope
  group by
    city,property_id
),
city_scope as (
select
  city,
  any_value(prix_au_m2_median_coloc_meuble) as prix_au_m2_median_coloc_meuble,
  any_value(prix_au_m2_median_coloc_non_meuble) as prix_au_m2_median_coloc_non_meuble,
  any_value(prix_au_m2_median_studio_meuble) as prix_au_m2_median_studio_meuble,
  any_value(prix_au_m2_median_studio_non_meuble) as prix_au_m2_median_studio_non_meuble,
  any_value(prix_au_m2_median_t2_meuble) as prix_au_m2_median_t2_meuble,
  any_value(prix_au_m2_median_t2_non_meuble) as prix_au_m2_median_t2_non_meuble,
  any_value(prix_au_m2_median_t3_meuble) as prix_au_m2_median_t3_meuble,
  any_value(prix_au_m2_median_t3_non_meuble) as prix_au_m2_median_t3_non_meuble,
  any_value(prix_au_m2_median_t4_plus_meuble) as prix_au_m2_median_t4_plus_meuble,
  any_value(prix_au_m2_median_t4_plus_non_meuble) as prix_au_m2_median_t4_plus_non_meuble,
  count(distinct property_id) as properties_volume
from
  (
    select
    city,
    property_id,
    round(percentile_cont(prix_au_m2_coloc_meuble,0.5) over w,2) as prix_au_m2_median_coloc_meuble,
    round(percentile_cont(prix_au_m2_coloc_non_meuble,0.5) over w,2) as prix_au_m2_median_coloc_non_meuble,
    round(percentile_cont(prix_au_m2_studio_meuble,0.5) over w,2) as prix_au_m2_median_studio_meuble,
    round(percentile_cont(prix_au_m2_studio_non_meuble,0.5) over w,2) as prix_au_m2_median_studio_non_meuble,
    round(percentile_cont(prix_au_m2_t2_meuble,0.5) over w,2) as prix_au_m2_median_t2_meuble,
    round(percentile_cont(prix_au_m2_t2_non_meuble,0.5) over w,2) as prix_au_m2_median_t2_non_meuble,
    round(percentile_cont(prix_au_m2_t3_meuble,0.5) over w,2) as prix_au_m2_median_t3_meuble,
    round(percentile_cont(prix_au_m2_t3_non_meuble,0.5) over w,2) as prix_au_m2_median_t3_non_meuble,
    round(percentile_cont(prix_au_m2_t4_plus_meuble,0.5) over w,2) as prix_au_m2_median_t4_plus_meuble,
    round(percentile_cont(prix_au_m2_t4_plus_non_meuble,0.5) over w,2) as prix_au_m2_median_t4_plus_non_meuble
    from
    property_scope
    window w as (
      partition by city
    )
  )
group by
	city
)
select
  *
from
  city_scope
order by
  properties_volume desc
limit 500

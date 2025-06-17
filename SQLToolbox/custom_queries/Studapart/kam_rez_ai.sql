WITH residences AS (
    SELECT DISTINCT
        re.id AS residence_id,
        FIRST_VALUE(co.name) OVER (PARTITION BY re.id ORDER BY re.created_at, re.updated_at, co.date_created, co.updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS residence_name,
    FROM
        `big-query-328314.db_data.mysql_residences_raw_data` AS re
    LEFT JOIN
        `big-query-328314.db_data.mysql_companies_raw_data` AS co
    ON
        SAFE_CAST(re.company_id AS INT64) = SAFE_CAST(co.id AS INT64)
),

property_online_during_date_range_current AS (
    SELECT
        main_availability_id,
        COUNT(DISTINCT date) AS days_online_during_date_range,
        CASE WHEN MAX(CASE WHEN date = PARSE_DATE("%Y%m%d", @DS_END_DATE) THEN 1 ELSE 0 END) = 1 THEN True ELSE False END AS availability_online_end_of_date_range
    FROM
        `big-query-328314.analysis_data.availabilities_visibility`
    WHERE
        date BETWEEN PARSE_DATE("%Y%m%d", @DS_START_DATE) AND PARSE_DATE("%Y%m%d", @DS_END_DATE)
        AND availability_is_online
    GROUP BY
        main_availability_id
),

property_pageviews_data_current AS (
    SELECT DISTINCT
        property_id,
        SUM(pageviews) AS pageviews,
        SUM(unique_pageviews) AS unique_pageviews,
    FROM
        `big-query-328314.analytics_data.preanalysis_ga_layer4_pages_property`
    WHERE
        DATE(date) BETWEEN PARSE_DATE("%Y%m%d", @DS_START_DATE) AND PARSE_DATE("%Y%m%d", @DS_END_DATE)
        AND property_id IS NOT NULL
    GROUP BY
        property_id
),

residence_pageviews_data_current AS (
    SELECT
        SAFE_CAST(residence_id AS INT64) AS residence_id,
        SUM(pageviews) AS pageviews,
        SUM(unique_pageviews) AS unique_pageviews,
    FROM
        `big-query-328314.analytics_data.preanalysis_ga_layer4_pages_property`
    WHERE
        DATE(date) BETWEEN PARSE_DATE("%Y%m%d", @DS_START_DATE) AND PARSE_DATE("%Y%m%d", @DS_END_DATE)
        AND SAFE_CAST(residence_id AS INT64) IS NOT NULL
    GROUP BY
        residence_id
),

property_online_during_date_range_compared AS (
    SELECT
        main_availability_id,
        COUNT(DISTINCT date) AS days_online_during_date_range,
        CASE WHEN MAX(CASE WHEN date = @compared_date_end THEN 1 ELSE 0 END) = 1 THEN True ELSE False END AS availability_online_end_of_date_range
    FROM
        `big-query-328314.analysis_data.availabilities_visibility`
    WHERE
        date BETWEEN @compared_date_start AND @compared_date_end
        AND availability_is_online
    GROUP BY
        main_availability_id
),

property_pageviews_data_compared AS (
    SELECT DISTINCT
        property_id,
        SUM(pageviews) AS pageviews,
        SUM(unique_pageviews) AS unique_pageviews,
    FROM
        `big-query-328314.analytics_data.preanalysis_ga_layer4_pages_property`
    WHERE
        DATE(date) BETWEEN @compared_date_start AND @compared_date_end
        AND property_id IS NOT NULL
    GROUP BY
        property_id
),

residence_pageviews_data_compared AS (
    SELECT
        SAFE_CAST(residence_id AS INT64) AS residence_id,
        SUM(pageviews) AS pageviews,
        SUM(unique_pageviews) AS unique_pageviews,
    FROM
        `big-query-328314.analytics_data.preanalysis_ga_layer4_pages_property`
    WHERE
        DATE(date) BETWEEN @compared_date_start AND @compared_date_end
        AND SAFE_CAST(residence_id AS INT64) IS NOT NULL
    GROUP BY
        residence_id
),

agencies as (
    select distinct
        property_id,
        last_value(owner.role.agency_name) over w as name,
        last_value(owner.role.is_allotted) over w as is_allotted
    from 
        `big-query-328314.db_data.preanalysis_db_properties`
    where
        owner.role.is_agency
    window w as (
        partition by property_id order by property_dates.updated_at asc
    )
)

select
  res.residence_id AS id,
  r.residence_name AS name,
  res.is_allotted,
  pageviews,
  cast(null as int64) as demandes,
  cast(null as int64) as allotes,
  cast(null as int64) as propositions,
  cast(null as int64) as reservations,
  'REZ' as bu,
  'current' as date_range
FROM
    `big-query-328314.db_data.preanalysis_db_residences` AS res
LEFT JOIN
    residences r
USING
    (residence_id)
LEFT JOIN
	residence_pageviews_data_current
ON
	res.residence_id = residence_pageviews_data_current.residence_id   
WHERE
    r.residence_name IS NOT NULL

union all

select
  accomodation.residence_id as id,
  accomodation.residence_name as name,
  accomodation.is_allotted,
  cast(null as int64) as pageviews,
  count(distinct request.id) as demandes,
  cast(null as int64) as allotes,
  cast(null as int64) as propositions,
  cast(null as int64) as reservations,
  'REZ' as bu,
  'current' as date_range
from
  unnest(
      generate_date_array(
          PARSE_DATE("%Y%m%d", @DS_START_DATE),
          PARSE_DATE("%Y%m%d", @DS_END_DATE)
      )
  ) date
  left join `big-query-328314.analysis_data.pipeline_mobilities_requests` r on date = date(r.request.created_at)
where
  r.accomodation.residence_name is not null
group by
  id,name,is_allotted

union all

select
  accomodation.residence_id as id,
  accomodation.residence_name as name,
  accomodation.is_allotted,
  cast(null as int64) as pageviews,
  cast(null as int64) as demandes,
  count(distinct case when mobility.is_for_allotted_accomodations then mobility.id else null end) as allotes,
  cast(null as int64) as propositions,
  cast(null as int64) as reservations,
  'REZ' as bu,
  'current' as date_range
from
  unnest(
      generate_date_array(
          PARSE_DATE("%Y%m%d", @DS_START_DATE),
          PARSE_DATE("%Y%m%d", @DS_END_DATE)
      )
  ) date
  left join `big-query-328314.analysis_data.pipeline_mobilities_requests` m on date = date(m.mobility.created_at)
where
  m.accomodation.residence_name is not null
group by
  id,name,is_allotted  

union all

select
  accomodation.residence_id as id,
  accomodation.residence_name as name,
  accomodation.is_allotted,
  cast(null as int64) as pageviews,
  cast(null as int64) as demandes,
  cast(null as int64) as allotes,
  count(distinct request.id) as propositions,
  cast(null as int64) as reservations,  
  'REZ' as bu,
  'current' as date_range
from
  unnest(
      generate_date_array(
          PARSE_DATE("%Y%m%d", @DS_START_DATE),
          PARSE_DATE("%Y%m%d", @DS_END_DATE)
      )
  ) date
  left join `big-query-328314.analysis_data.pipeline_mobilities_requests` da on date = date(da.owner_proposition_created_at)
where
  da.accomodation.residence_name is not null
group by
  id,name,is_allotted  

union all

select
  accomodation.residence_id as id,
  accomodation.residence_name as name,
  accomodation.is_allotted,
  cast(null as int64) as pageviews,
  cast(null as int64) as demandes,
  cast(null as int64) as allotes,
  cast(null as int64) as propositions,
  count(distinct request.id) as reservations,   
  'REZ' as bu,
  'current' as date_range
from
  unnest(
      generate_date_array(
          PARSE_DATE("%Y%m%d", @DS_START_DATE),
          PARSE_DATE("%Y%m%d", @DS_END_DATE)
      )
  ) date
  left join `big-query-328314.analysis_data.pipeline_mobilities_requests` b on date = date(b.request.won_at)
where
  b.accomodation.residence_name is not null
group by
  id,name,is_allotted    

union all

select
  cast(null as int64) as id,
  a.name,
  a.is_allotted,
  pageviews,
  cast(null as int64) as demandes,
  cast(null as int64) as allotes,
  cast(null as int64) as propositions,
  cast(null as int64) as reservations,
  'AI' as bu,
  'current' as date_range
FROM
	`big-query-328314.analysis_data.analysis_availabilities` aa
LEFT JOIN
	property_pageviews_data_current
ON
	aa.property.id = property_pageviews_data_current.property_id
LEFT JOIN
    agencies a
ON
    aa.property.id = a.property_id
LEFT JOIN
	property_online_during_date_range_current
ON
	aa.main_availability.id = property_online_during_date_range_current.main_availability_id
WHERE
    property_online_during_date_range_current.main_availability_id IS NOT NULL    
    AND
    aa.owner.role.is_agency

union all

select
  cast(null as int64) as id,
  accomodation.owner.role.agency_name as name,
  accomodation.is_allotted,
  cast(null as int64) as pageviews,
  count(distinct request.id) as demandes,
  cast(null as int64) as allotes,
  cast(null as int64) as propositions,
  cast(null as int64) as reservations,  
  'AI' as bu,
  'current' as date_range
from
  unnest(
      generate_date_array(
          PARSE_DATE("%Y%m%d", @DS_START_DATE),
          PARSE_DATE("%Y%m%d", @DS_END_DATE)
      )
  ) date
  left join `big-query-328314.analysis_data.pipeline_mobilities_requests` r on date = date(r.request.created_at)
where
  r.accomodation.owner.role.is_agency
group by
  id,name,is_allotted  

union all

select
  cast(null as int64) as id,
  accomodation.owner.role.agency_name as name,
  accomodation.is_allotted,
  cast(null as int64) as pageviews,
  cast(null as int64) as demandes,
  count(distinct case when mobility.is_for_allotted_accomodations then mobility.id else null end) as allotes,
  cast(null as int64) as propositions,
  cast(null as int64) as reservations,  
  'AI' as bu,
  'current' as date_range
from
  unnest(
      generate_date_array(
          PARSE_DATE("%Y%m%d", @DS_START_DATE),
          PARSE_DATE("%Y%m%d", @DS_END_DATE)
      )
  ) date
  left join `big-query-328314.analysis_data.pipeline_mobilities_requests` m on date = date(m.mobility.created_at)
where
  m.accomodation.owner.role.is_agency
group by
  id,name,is_allotted  

union all

select
  cast(null as int64) as id,
  accomodation.owner.role.agency_name as name,
  accomodation.is_allotted,
  cast(null as int64) as pageviews,
  cast(null as int64) as demandes,
  cast(null as int64) as allotes,
  count(distinct request.id) as propositions,
  cast(null as int64) as reservations,  
  'AI' as bu,
  'current' as date_range
from
  unnest(
      generate_date_array(
          PARSE_DATE("%Y%m%d", @DS_START_DATE),
          PARSE_DATE("%Y%m%d", @DS_END_DATE)
      )
  ) date
  left join `big-query-328314.analysis_data.pipeline_mobilities_requests` da on date = date(da.owner_proposition_created_at)
where
  da.accomodation.owner.role.is_agency
group by
  id,name,is_allotted  

union all

select
  cast(null as int64) as id,
  accomodation.owner.role.agency_name as name,
  accomodation.is_allotted,
  cast(null as int64) as pageviews,
  cast(null as int64) as demandes,
  cast(null as int64) as allotes,
  cast(null as int64) as propositions,
  count(distinct request.id) as reservations,  
  'AI' as bu,
  'current' as date_range
from
  unnest(
      generate_date_array(
          PARSE_DATE("%Y%m%d", @DS_START_DATE),
          PARSE_DATE("%Y%m%d", @DS_END_DATE)
      )
  ) date
  left join `big-query-328314.analysis_data.pipeline_mobilities_requests` b on date = date(b.request.won_at)
where
  b.accomodation.owner.role.is_agency
group by
  id,name,is_allotted

union all

select
  res.residence_id AS id,
  r.residence_name AS name,
  res.is_allotted,
  pageviews,
  cast(null as int64) as demandes,
  cast(null as int64) as allotes,
  cast(null as int64) as propositions,
  cast(null as int64) as reservations,   
  'REZ' as bu,
  'compared' as date_range
FROM
    `big-query-328314.db_data.preanalysis_db_residences` AS res
LEFT JOIN
    residences r
USING
    (residence_id)
LEFT JOIN
	residence_pageviews_data_compared
ON
	res.residence_id = residence_pageviews_data_compared.residence_id   
WHERE
    r.residence_name IS NOT NULL

union all

select
  accomodation.residence_id as id,
  accomodation.residence_name as name,
  accomodation.is_allotted,
  cast(null as int64) as pageviews,
  count(distinct request.id) as demandes,
  cast(null as int64) as allotes,
  cast(null as int64) as propositions,
  cast(null as int64) as reservations,
  'REZ' as bu,
  'compared' as date_range
from
  unnest(
      generate_date_array(
          @compared_date_start,
          @compared_date_end
      )
  ) date
  left join `big-query-328314.analysis_data.pipeline_mobilities_requests` r on date = date(r.request.created_at)
where
  r.accomodation.residence_name is not null
group by
  id,name,is_allotted

union all

select
  accomodation.residence_id as id,
  accomodation.residence_name as name,
  accomodation.is_allotted,
  cast(null as int64) as pageviews, 
  cast(null as int64) as demandes,
  count(distinct case when mobility.is_for_allotted_accomodations then mobility.id else null end) as allotes,
  cast(null as int64) as propositions,
  cast(null as int64) as reservations,
  'REZ' as bu,
  'compared' as date_range 
from
  unnest(
      generate_date_array(
          @compared_date_start,
          @compared_date_end
      )
  ) date
  left join `big-query-328314.analysis_data.pipeline_mobilities_requests` m on date = date(m.mobility.created_at)
where
  m.accomodation.residence_name is not null
group by
  id,name,is_allotted  

union all

select
  accomodation.residence_id as id,
  accomodation.residence_name as name,
  accomodation.is_allotted,
  cast(null as int64) as pageviews, 
  cast(null as int64) as demandes,
  cast(null as int64) as allotes,
  count(distinct request.id) as propositions,
  cast(null as int64) as reservations,  
  'REZ' as bu,
  'compared' as date_range
from
  unnest(
      generate_date_array(
          @compared_date_start,
          @compared_date_end
      )
  ) date
  left join `big-query-328314.analysis_data.pipeline_mobilities_requests` da on date = date(da.owner_proposition_created_at)
where
  da.accomodation.residence_name is not null
group by
  id,name,is_allotted  

union all

select
  accomodation.residence_id as id,
  accomodation.residence_name as name,
  accomodation.is_allotted,
  cast(null as int64) as pageviews,
  cast(null as int64) as demandes,
  cast(null as int64) as allotes,
  cast(null as int64) as propositions,
  count(distinct request.id) as reservations,   
  'REZ' as bu,
  'compared' as date_range
from
  unnest(
      generate_date_array(
          @compared_date_start,
          @compared_date_end
      )
  ) date
  left join `big-query-328314.analysis_data.pipeline_mobilities_requests` b on date = date(b.request.won_at)
where
  b.accomodation.residence_name is not null
group by
  id,name,is_allotted    

union all

select
  cast(null as int64) as id,
  a.name,
  a.is_allotted,
  pageviews,
  cast(null as int64) as demandes,
  cast(null as int64) as allotes,
  cast(null as int64) as propositions,
  cast(null as int64) as reservations,
  'AI' as bu,
  'compared' as date_range
FROM
	`big-query-328314.analysis_data.analysis_availabilities` aa
LEFT JOIN
	property_pageviews_data_compared
ON
	aa.property.id = property_pageviews_data_compared.property_id
LEFT JOIN
    agencies a
ON
    aa.property.id = a.property_id
LEFT JOIN
	property_online_during_date_range_compared
ON
	aa.main_availability.id = property_online_during_date_range_compared.main_availability_id
WHERE
    property_online_during_date_range_compared.main_availability_id IS NOT NULL    
    AND
    aa.owner.role.is_agency

union all

select
  cast(null as int64) as id,
  accomodation.owner.role.agency_name as name,
  accomodation.is_allotted,
  cast(null as int64) as pageviews,
  count(distinct request.id) as demandes,
  cast(null as int64) as allotes,
  cast(null as int64) as propositions,
  cast(null as int64) as reservations,  
  'AI' as bu,
  'compared' as date_range 
from
  unnest(
      generate_date_array(
          @compared_date_start,
          @compared_date_end
      )
  ) date
  left join `big-query-328314.analysis_data.pipeline_mobilities_requests` r on date = date(r.request.created_at)
where
  r.accomodation.owner.role.is_agency
group by
  id,name,is_allotted  

union all

select
  cast(null as int64) as id,
  accomodation.owner.role.agency_name as name,
  accomodation.is_allotted,
  cast(null as int64) as pageviews, 
  cast(null as int64) as demandes,
  count(distinct case when mobility.is_for_allotted_accomodations then mobility.id else null end) as allotes,
  cast(null as int64) as propositions,
  cast(null as int64) as reservations,  
  'AI' as bu,
  'compared' as date_range
from
  unnest(
      generate_date_array(
          @compared_date_start,
          @compared_date_end
      )
  ) date
  left join `big-query-328314.analysis_data.pipeline_mobilities_requests` m on date = date(m.mobility.created_at)
where
  m.accomodation.owner.role.is_agency
group by
  id,name,is_allotted  

union all

select
  cast(null as int64) as id,
  accomodation.owner.role.agency_name as name,
  accomodation.is_allotted,
  cast(null as int64) as pageviews,
  cast(null as int64) as demandes,
  cast(null as int64) as allotes,
  count(distinct request.id) as propositions,
  cast(null as int64) as reservations,  
  'AI' as bu,
  'compared' as date_range
from
  unnest(
      generate_date_array(
          @compared_date_start,
          @compared_date_end
      )
  ) date
  left join `big-query-328314.analysis_data.pipeline_mobilities_requests` da on date = date(da.owner_proposition_created_at)
where
  da.accomodation.owner.role.is_agency
group by
  id,name,is_allotted  

union all

select
  cast(null as int64) as id,
  accomodation.owner.role.agency_name as name,
  accomodation.is_allotted,
  cast(null as int64) as pageviews,
  cast(null as int64) as demandes,
  cast(null as int64) as allotes,
  cast(null as int64) as propositions,
  count(distinct request.id) as reservations,  
  'AI' as bu,
  'compared' as date_range
from
  unnest(
      generate_date_array(
          @compared_date_start,
          @compared_date_end
      )
  ) date
  left join `big-query-328314.analysis_data.pipeline_mobilities_requests` b on date = date(b.request.won_at)
where
  b.accomodation.owner.role.is_agency
group by
  id,name,is_allotted

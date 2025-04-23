SELECT
  ba,
  country_name,
  device,
  business_type,
  banner_name,

  nb_optin,
  nb_optout,
  nb_banner_views,

  CAST(NULL AS INT64) AS nb_optin_previous_year, 
  CAST(NULL AS INT64) AS nb_optout_previous_year,
  CAST(NULL AS INT64) AS nb_banner_views_previous_year,

  CAST(NULL AS INT64) AS nb_optin_previous_period,
  CAST(NULL AS INT64) AS nb_optout_previous_period,
  CAST(NULL AS INT64) AS nb_banner_views_previous_period
FROM 
	`somfy-data.dataform.consent_dashboard` data
WHERE date between PARSE_DATE('%Y%m%d',  @DS_START_DATE) AND PARSE_DATE('%Y%m%d',  @DS_END_DATE)

UNION ALL 

SELECT
  ba,
  country_name,
  device,
  business_type,
  banner_name,

  CAST(NULL AS INT64) AS nb_optin,
  CAST(NULL AS INT64) AS nb_optout,
  CAST(NULL AS INT64) AS nb_banner_views,

  nb_optin AS nb_optin_previous_year, 
  nb_optout AS nb_optout_previous_year,
  nb_banner_views AS nb_banner_views_previous_year,

  CAST(NULL AS INT64) AS nb_optin_previous_period,
  CAST(NULL AS INT64) AS nb_optout_previous_period,
  CAST(NULL AS INT64) AS nb_banner_views_previous_period
FROM 
  `somfy-data.dataform.consent_dashboard` data
 WHERE date between date_sub(PARSE_DATE('%Y%m%d',  @DS_START_DATE),INTERVAL 1 YEAR) AND date_sub(PARSE_DATE('%Y%m%d',  @DS_END_DATE),INTERVAL 1 YEAR)

UNION ALL 

SELECT
  ba,
  country_name,
  device,
  business_type,
  banner_name,

  CAST(NULL AS INT64) AS nb_optin,
  CAST(NULL AS INT64) AS nb_optout,
  CAST(NULL AS INT64) AS nb_banner_views,

  CAST(NULL AS INT64) AS nb_optin_previous_year, 
  CAST(NULL AS INT64) AS nb_optout_previous_year,
  CAST(NULL AS INT64) AS nb_banner_views_previous_year,

  nb_optin AS nb_optin_previous_period,
  nb_optout AS nb_optout_previous_period,
  nb_banner_views AS nb_banner_views_previous_period
	
FROM 
  `somfy-data.dataform.consent_dashboard` data
 WHERE date between date_sub(PARSE_DATE('%Y%m%d',  @DS_START_DATE),INTERVAL DATE_DIFF(PARSE_DATE('%Y%m%d',  @DS_END_DATE),PARSE_DATE('%Y%m%d',  @DS_START_DATE),DAY)+1 DAY) AND date_sub(PARSE_DATE('%Y%m%d',  @DS_START_DATE),INTERVAL 1 DAY)   

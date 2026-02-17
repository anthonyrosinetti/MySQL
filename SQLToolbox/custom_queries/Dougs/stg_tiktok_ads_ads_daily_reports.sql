SELECT
    _airbyte_extracted_at AS timestamp_last_refresh,
    DATE(stat_time_day) AS date_report,
    LAX_STRING(JSON_QUERY(metrics, "$.campaign_id")) AS campaign_id,
    LAX_STRING(JSON_QUERY(metrics, "$.adgroup_id")) AS adgroup_id,
    CAST(ad_id AS STRING) AS ad_id,
    LAX_INT64(JSON_QUERY(metrics, "$.impressions")) AS impressions,
    LAX_INT64(JSON_QUERY(metrics, "$.clicks")) AS clicks,
    LAX_FLOAT64(JSON_QUERY(metrics, "$.spend")) AS spend
FROM
    {{ source('bronze_airbyte_tiktok', 'ads_reports_daily') }}

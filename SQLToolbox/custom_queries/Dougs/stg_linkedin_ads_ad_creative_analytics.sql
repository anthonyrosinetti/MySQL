SELECT
    _airbyte_extracted_at AS timestamp_last_refresh,
    start_date AS date_report,
    CAST(sponsoredCreative AS STRING) AS creative_id,
    impressions,
    clicks,
    costInLocalCurrency AS spend
FROM
    {{ source('bronze_airbyte_linkedin_ads', 'ad_creative_analytics') }}

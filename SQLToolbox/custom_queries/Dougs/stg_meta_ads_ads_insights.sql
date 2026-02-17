SELECT
    _airbyte_extracted_at AS timestamp_last_refresh,
    date_start AS date_report,
    CAST(account_id AS STRING) AS account_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    CAST(adset_id AS STRING) AS adset_id,
    CAST(ad_id AS STRING) AS ad_id,
    impressions,
    clicks,
    spend
FROM
    {{ source('bronze_airbyte_facebook_ads', 'ads_insights') }}

SELECT
    _airbyte_extracted_at AS timestamp_last_refresh,
    start_date AS date_report,
    CAST(sponsoredCampaign AS STRING) AS campaign_id,
    impressions,
    clicks,
    costInLocalCurrency AS spend
FROM
    {{ source('bronze_airbyte_linkedin_ads', 'ad_campaign_analytics') }}

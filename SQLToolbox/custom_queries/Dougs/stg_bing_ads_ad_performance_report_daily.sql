SELECT
    _airbyte_extracted_at AS timestamp_last_refresh,
    TimePeriod AS date_report,
    CAST(AccountId AS STRING) AS account_id,
    CAST(CampaignId AS STRING) AS campaign_id,
    CAST(AdGroupId AS STRING) AS ad_group_id,
    CAST(AdId AS STRING) AS ad_id,
    AdDescription AS ad_description,
    DeviceType AS device_type,
    Impressions AS impressions,
    Clicks AS clicks,
    Spend AS spend
FROM
    {{ source('bronze_airbyte_bing_ads', 'ad_performance_report_daily') }}

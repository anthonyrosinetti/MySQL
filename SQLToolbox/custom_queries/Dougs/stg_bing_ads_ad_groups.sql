SELECT
    _airbyte_extracted_at AS timestamp_last_refresh,
    CAST(AccountId AS STRING) AS account_id,
    CAST(CampaignId AS STRING) AS campaign_id,
    CAST(Id AS STRING) AS ad_group_id,
    Name AS ad_group_name,
    AdGroupType AS ad_group_type
FROM
    {{ source('bronze_airbyte_bing_ads', 'ad_groups') }}

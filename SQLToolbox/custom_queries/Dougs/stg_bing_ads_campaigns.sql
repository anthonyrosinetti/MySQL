SELECT
    _airbyte_extracted_at AS timestamp_last_refresh,
    CAST(AccountId AS STRING) AS account_id,
    CAST(Id AS STRING) AS campaign_id,
    Name AS campaign_name,
    CampaignType AS campaign_type
FROM
    {{ source('bronze_airbyte_bing_ads', 'campaigns') }}

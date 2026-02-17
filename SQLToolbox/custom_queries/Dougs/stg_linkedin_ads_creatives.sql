SELECT
    _airbyte_extracted_at AS timestamp_last_refresh,
    account AS account_urn,
    campaign AS campaign_urn,
    id AS creative_urn,
    name AS creative_name
FROM
    {{ source('bronze_airbyte_linkedin_ads', 'creatives') }}

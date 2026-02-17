SELECT
    _airbyte_extracted_at AS timestamp_last_refresh,
    account AS account_urn,
    CAST(id AS STRING) AS campaign_group_id,
    name AS campaign_group_name
FROM
    {{ source('bronze_airbyte_linkedin_ads', 'campaign_groups') }}

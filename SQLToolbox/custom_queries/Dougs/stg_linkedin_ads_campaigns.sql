SELECT
    _airbyte_extracted_at AS timestamp_last_refresh,
    account AS account_urn,
    campaignGroup AS campaign_group_urn,
    CAST(id AS STRING) AS campaign_id,
    name AS campaign_name
FROM
    {{ source('bronze_airbyte_linkedin_ads', 'campaigns') }}

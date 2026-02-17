SELECT
    _airbyte_extracted_at AS timestamp_last_refresh,
    CAST(campaign_id AS STRING) AS campaign_id,
    campaign_name
FROM
    {{ source('bronze_airbyte_tiktok', 'campaigns') }}

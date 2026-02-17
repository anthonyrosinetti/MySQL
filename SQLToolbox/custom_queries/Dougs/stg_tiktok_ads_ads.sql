SELECT
    _airbyte_extracted_at AS timestamp_last_refresh,
    CAST(ad_id AS STRING) AS ad_id,
    ad_name
FROM
    {{ source('bronze_airbyte_tiktok', 'ads') }}

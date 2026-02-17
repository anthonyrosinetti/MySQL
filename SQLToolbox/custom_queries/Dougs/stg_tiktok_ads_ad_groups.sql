SELECT
    _airbyte_extracted_at AS timestamp_last_refresh,
    CAST(adgroup_id AS STRING) AS adgroup_id,
    adgroup_name
FROM
    {{ source('bronze_airbyte_tiktok', 'ad_groups') }}

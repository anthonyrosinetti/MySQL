SELECT
    _airbyte_extracted_at AS timestamp_last_refresh,
    CAST(account_id AS STRING) AS account_id,
    CAST(id AS STRING) AS ad_id,
    name AS ad_name
FROM
    {{ source('bronze_airbyte_facebook_ads', 'ads') }}

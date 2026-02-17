SELECT
    _airbyte_extracted_at AS timestamp_last_refresh,
    CAST(Id AS STRING) AS account_id,
    Name AS account_name,
    CurrencyCode AS account_currency
FROM
    {{ source('bronze_airbyte_bing_ads', 'accounts') }}

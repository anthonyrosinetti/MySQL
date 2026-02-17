-- Keeping only the latest data per account to fit with what's available in the platform UI
SELECT
    timestamp_last_refresh,
    account_id,
    account_name,
    account_currency
FROM
    {{ ref('stg_bing_ads_accounts') }}
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY account_id
        ORDER BY timestamp_last_refresh DESC
    ) = 1

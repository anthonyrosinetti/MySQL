-- Keeping only the latest data per ad to fit with what's available in the platform UI
SELECT
    timestamp_last_refresh,
    account_id,
    ad_id,
    ad_name
FROM
    {{ ref('stg_meta_ads_ads') }}
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY ad_id
        ORDER BY timestamp_last_refresh DESC
    ) = 1

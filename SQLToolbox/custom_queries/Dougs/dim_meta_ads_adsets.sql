-- Keeping only the latest data per ad set to fit with what's available in the platform UI
SELECT
    timestamp_last_refresh,
    account_id,
    adset_id,
    adset_name
FROM
    {{ ref('stg_meta_ads_ad_sets') }}
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY adset_id
        ORDER BY timestamp_last_refresh DESC
    ) = 1

-- Keeping only the latest data per ad group to fit with what's available in the platform UI
SELECT
    timestamp_last_refresh,
    adgroup_id,
    adgroup_name
FROM
    {{ ref('stg_tiktok_ads_ad_groups') }}
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY adgroup_id
        ORDER BY timestamp_last_refresh DESC
    ) = 1

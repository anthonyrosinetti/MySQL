-- Keeping only the latest data per ad group to fit with what's available in the platform UI
SELECT
    timestamp_last_refresh,
    account_id,
    campaign_id,
    ad_group_id,
    ad_group_name,
    ad_group_type
FROM
    {{ ref('stg_bing_ads_ad_groups') }}
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY account_id, campaign_id, ad_group_id
        ORDER BY timestamp_last_refresh DESC
    ) = 1

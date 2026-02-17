-- Keeping only the latest data per campaign to fit with what's available in the platform UI
SELECT
    timestamp_last_refresh,
    account_id,
    campaign_id,
    campaign_name
FROM
    {{ ref('stg_meta_ads_campaigns') }}
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY campaign_id
        ORDER BY timestamp_last_refresh DESC
    ) = 1

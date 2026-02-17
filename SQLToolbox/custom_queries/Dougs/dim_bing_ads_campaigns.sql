-- Keeping only the latest data per campaign to fit with what's available in the platform UI
SELECT
    timestamp_last_refresh,
    account_id,
    campaign_id,
    campaign_name,
    CASE
        WHEN campaign_type = "PerformanceMax" THEN "Performance Max"
        ELSE campaign_type
    END AS campaign_type
FROM
    {{ ref('stg_bing_ads_campaigns') }}
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY account_id, campaign_id
        ORDER BY timestamp_last_refresh DESC
    ) = 1

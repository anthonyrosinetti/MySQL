-- Keeping only the latest data per keyword to fit with what's available in the platform UI
SELECT
    timestamp_last_refresh,
    account_id,
    campaign_id,
    ad_group_id,
    keyword_id,
    keyword_name
FROM
    {{ ref('stg_bing_ads_keyword_performance_report_daily') }}
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY account_id, campaign_id, ad_group_id, keyword_id
        ORDER BY timestamp_last_refresh DESC, date_report DESC
    ) = 1

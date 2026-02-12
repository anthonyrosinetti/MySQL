-- Keeping only the latest data per ad to fit with what's available in the platform UI
SELECT
    date_last_refresh,
    customer_id,
    campaign_id,
    ad_group_id,
    ad_id,
    ad_name
FROM
    ${ref("stg_google_ads_ad")}
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY customer_id, campaign_id, ad_group_id, ad_id
        ORDER BY date_last_refresh DESC
    ) = 1

-- Keeping only the latest data per keyword to fit with what's available in the platform UI
SELECT
    date_last_refresh,
    customer_id,
    campaign_id,
    ad_group_id,
    keyword_id,
    keyword_name
FROM
    ${ref("stg_google_ads_keyword")}
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY customer_id, campaign_id, ad_group_id, keyword_id
        ORDER BY date_last_refresh DESC
    ) = 1

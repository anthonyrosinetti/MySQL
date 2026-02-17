SELECT
    date_report,
    account_id,
    campaign_id,
    adset_id,
    ad_id,
    NULLIF(SUM(impressions), 0) AS impressions,
    NULLIF(SUM(clicks), 0) AS clicks,
    NULLIF(SUM(spend), 0) AS spend
FROM
    {{ ref('stg_meta_ads_ads_insights') }}
GROUP BY
    date_report,
    account_id,
    campaign_id,
    adset_id,
    ad_id
HAVING
    impressions IS NOT NULL
    OR clicks IS NOT NULL
    OR spend IS NOT NULL

SELECT
    date_report,
    campaign_id,
    adgroup_id,
    ad_id,
    NULLIF(SUM(impressions), 0) AS impressions,
    NULLIF(SUM(clicks), 0) AS clicks,
    NULLIF(SUM(spend), 0) AS spend
FROM
    {{ ref('stg_tiktok_ads_ads_daily_reports') }}
GROUP BY
    date_report,
    campaign_id,
    adgroup_id,
    ad_id
HAVING
    impressions IS NOT NULL
    OR clicks IS NOT NULL
    OR spend IS NOT NULL

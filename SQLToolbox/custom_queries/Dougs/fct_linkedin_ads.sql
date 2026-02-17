WITH all_ad_account_levels AS (
    SELECT
        dat.date_report AS date_report,
        cam.account_id AS account_id,
        cam.campaign_group_id AS campaign_group_id,
        dat.campaign_id,
        CAST(NULL AS STRING) AS creative_id,
        dat.impressions AS impressions,
        dat.clicks AS clicks,
        dat.spend AS spend
    FROM
        {{ ref('stg_linkedin_ads_ad_campaign_analytics') }} AS dat
    LEFT JOIN
        {{ ref('dim_linkedin_ads_campaigns') }} AS cam
    ON
        dat.campaign_id = cam.campaign_id

    UNION ALL

    SELECT
        dat.date_report AS date_report,
        cre.account_id AS account_id,
        cam.campaign_group_id AS campaign_group_id,
        cre.campaign_id,
        CASE WHEN deduplication_multiplicator = 1 THEN dat.creative_id END AS creative_id,
        deduplication_multiplicator * dat.impressions AS impressions,
        deduplication_multiplicator * dat.clicks AS clicks,
        deduplication_multiplicator * dat.spend AS spend
    FROM
        {{ ref('stg_linkedin_ads_ad_creative_analytics') }} AS dat
    LEFT JOIN
        {{ ref('dim_linkedin_ads_creatives') }} AS cre
    ON
        dat.creative_id = cre.creative_id
    LEFT JOIN
        {{ ref('dim_linkedin_ads_campaigns') }} AS cam
    ON
        cre.campaign_id = cam.campaign_id
    -- Creative performance data are also counted at the campaign level, even though the precision is more accurate at the campaign level
    -- adding the creatibe level without changing total at the campaign level
    -- using CROSS JOIN to duplicate the data, once with a +1 multiplicator, once with a -1 multiplicator, so the total is 0 (ie. not impacting the overall total)
    -- when multiplicator is +1, adding the creative granularity dimension
    CROSS JOIN
        UNNEST([-1, 1]) AS deduplication_multiplicator
)

SELECT
    date_report,
    account_id,
    campaign_group_id,
    campaign_id,
    creative_id,
    NULLIF(SUM(impressions), 0) AS impressions,
    NULLIF(SUM(clicks), 0) AS clicks,
    NULLIF(SUM(spend), 0) AS spend
FROM
    all_ad_account_levels
GROUP BY
    date_report,
    account_id,
    campaign_group_id,
    campaign_id,
    creative_id
HAVING
    impressions IS NOT NULL
    OR clicks IS NOT NULL
    OR spend IS NOT NULL

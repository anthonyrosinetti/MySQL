WITH all_ad_account_levels AS (
    SELECT
        date_report,
        account_id,
        campaign_id,
        CAST(NULL AS STRING) AS ad_group_id,
        CAST(NULL AS STRING) AS ad_id,
        CAST(NULL AS STRING) AS keyword_id,
        device_type,
        impressions,
        clicks,
        spend
    FROM
        {{ ref('stg_bing_ads_campaign_performance_report_daily') }}

    UNION ALL

    SELECT
        date_report,
        account_id,
        campaign_id,
        CASE WHEN deduplication_multiplicator = 1 THEN ad_group_id END AS ad_group_id,
        CAST(NULL AS STRING) AS ad_id,
        CAST(NULL AS STRING) AS keyword_id,
        device_type,
        deduplication_multiplicator * impressions AS impressions,
        deduplication_multiplicator * clicks AS clicks,
        deduplication_multiplicator * spend AS spend
    FROM
        {{ ref('stg_bing_ads_ad_group_performance_report_daily') }}
    -- Ad group performance data are also counted at the campaign level, even though the precision is more accurate at the campaign level
    -- adding the ad group level without changing total at the campaign level
    -- using CROSS JOIN to duplicate the data, once with a +1 multiplicator, once with a -1 multiplicator, so the total is 0 (ie. not impacting the overall total)
    -- when multiplicator is +1, adding the ad group granularity dimension
    CROSS JOIN
        UNNEST([-1, 1]) AS deduplication_multiplicator

    UNION ALL

    SELECT
        date_report,
        account_id,
        campaign_id,
        ad_group_id,
        CASE WHEN deduplication_multiplicator = 1 THEN ad_id END AS ad_id,
        CAST(NULL AS STRING) AS keyword_id,
        device_type,
        deduplication_multiplicator * impressions AS impressions,
        deduplication_multiplicator * clicks AS clicks,
        deduplication_multiplicator * spend AS spend
    FROM
        {{ ref('stg_bing_ads_ad_performance_report_daily') }}
    -- Ad performance data are also counted at the ad group level, even though the precision is more accurate at the ad group level
    -- adding the ad level without changing total at the ad group level
    -- using CROSS JOIN to duplicate the data, once with a +1 multiplicator, once with a -1 multiplicator, so the total is 0 (ie. not impacting the overall total)
    -- when multiplicator is +1, adding the ad granularity dimension
    CROSS JOIN
        UNNEST([-1, 1]) AS deduplication_multiplicator

    UNION ALL

    SELECT
        date_report,
        account_id,
        campaign_id,
        ad_group_id,
        ad_id,
        CASE WHEN deduplication_multiplicator = 1 THEN keyword_id END AS keyword_id,
        device_type,
        deduplication_multiplicator * impressions AS impressions,
        deduplication_multiplicator * clicks AS clicks,
        deduplication_multiplicator * spend AS spend
    FROM
        {{ ref('stg_bing_ads_keyword_performance_report_daily') }}
    -- Keyword performance data are also counted at the ad level, even though the precision is more accurate at the ad level
    -- adding the keyword level without changing total at the ad level
    -- using CROSS JOIN to duplicate the data, once with a +1 multiplicator, once with a -1 multiplicator, so the total is 0 (ie. not impacting the overall total)
    -- when multiplicator is +1, adding the keyword granularity dimension
    CROSS JOIN
        UNNEST([-1, 1]) AS deduplication_multiplicator
)

SELECT
    date_report,
    account_id,
    campaign_id,
    ad_group_id,
    ad_id,
    keyword_id,
    device_type,
    NULLIF(SUM(impressions), 0) AS impressions,
    NULLIF(SUM(clicks), 0) AS clicks,
    NULLIF(SUM(spend), 0) AS spend
FROM
    all_ad_account_levels
GROUP BY
    date_report,
    account_id,
    campaign_id,
    ad_group_id,
    ad_id,
    keyword_id,
    device_type
HAVING
    impressions IS NOT NULL
    OR clicks IS NOT NULL
    OR spend IS NOT NULL
